// Serviço Axum simples que recebe requisições HTTP e empurra os dados para uma fila
// bounded. Uma única task de background lê a fila em lotes, simulando um trabalho
// lento (gravar no disco, inserir no banco etc.). O objetivo é mostrar como separar
// ingestão rápida de processamento lento mantendo backpressure automático.
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::Deserialize;
use sqlx::{QueryBuilder, SqlitePool, sqlite::SqlitePoolOptions};
use std::{fs, net::SocketAddr, path::Path, time::Duration};
use tokio::sync::mpsc;

/// Estado compartilhado entre as requisições HTTP e a task de background.
/// Contém apenas o canal para empurrar novos trabalhos.
#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<Job>,
}

/// Representa um item que precisa ser processado pelo trabalhador.
/// Por enquanto só guarda uma string genérica, mas poderia ser qualquer payload.
#[derive(Debug)]
struct Job {
    payload: String,
}

/// Estrutura usada para desserializar o JSON recebido pela rota `/process`.
#[derive(Deserialize)]
struct IngestRequest {
    data: String,
}

/// Função "porteiro": recebe HTTP e só joga na fila para ser processado depois.
async fn ingest(State(state): State<AppState>, Json(req): Json<IngestRequest>) -> StatusCode {
    // converte o corpo HTTP em um Job para o trabalhador de background
    let job = Job { payload: req.data };

    // fila bounded + await == backpressure
    if state.tx.send(job).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    // igual ao Java: 202 Accepted
    StatusCode::ACCEPTED
}

/// Função "trabalhador": lê os Jobs da fila, junta em lotes e processa de uma vez.
/// 1. Garante que sempre pegue pelo menos um item antes de seguir.
/// 2. Junta até `BATCH_SIZE` elementos ou até expirar `LINGER_MS`.
/// 3. Processa o lote inteiro (simulado com um `println!`).
async fn worker(mut rx: mpsc::Receiver<Job>, pool: SqlitePool) {
    const BATCH_SIZE: usize = 1000; // limite de itens por lote
    const LINGER_MS: u64 = 10; // tempo máximo (ms) esperando para fechar o lote

    // buffer é reaproveitado para evitar realocações a cada lote
    let mut buffer = Vec::with_capacity(BATCH_SIZE);

    loop {
        buffer.clear();

        // pega pelo menos 1 item antes de iniciar o ciclo de coleta
        let first = match rx.recv().await {
            Some(job) => job,
            None => break, // canal fechado, encerra
        };
        buffer.push(first);

        // janela de coleta (linger): limite de tempo para ficar esperando novos itens
        let deadline = tokio::time::sleep(Duration::from_millis(LINGER_MS));
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(job) => {
                            buffer.push(job);
                            if buffer.len() >= BATCH_SIZE {
                                // atingimos o limite: processa imediatamente
                                break;
                            }
                        }
                        None => {
                            // canal acabou, processa o que tiver e sai
                            break;
                        }
                    }
                }
                _ = &mut deadline => {
                    // acabou nossa janela de coleta: processa o que já juntamos
                    break;
                }
            }
        }

        if let Err(err) = persist_batch(&pool, &buffer).await {
            eprintln!(
                "Falha ao persistir lote de {} itens no SQLite: {}",
                buffer.len(),
                err
            );
        } else {
            println!("Persistido lote de {} itens no SQLite", buffer.len());
        }
    }
}

async fn persist_batch(pool: &SqlitePool, batch: &[Job]) -> Result<(), sqlx::Error> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut query_builder = QueryBuilder::new("INSERT INTO jobs (payload) ");
    query_builder.push_values(batch, |mut b, job| {
        b.push_bind(&job.payload);
    });

    query_builder.build().execute(pool).await.map(|_| ())
}

fn ensure_sqlite_file(db_url: &str) -> std::io::Result<()> {
    if let Some(path) = db_url.strip_prefix("sqlite://") {
        let path = Path::new(path);

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }

        if !path.exists() {
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?;
        }
    }

    Ok(())
}

/// Inicializa o servidor HTTP e a task de background.
#[tokio::main]
async fn main() {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://jobs.db".into());
    ensure_sqlite_file(&database_url).expect("Falha ao preparar o arquivo jobs.db");

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Falha ao conectar no SQLite");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )",
    )
    .execute(&pool)
    .await
    .expect("Falha ao criar tabela jobs");

    // fila bounded para backpressure
    let (tx, rx) = mpsc::channel::<Job>(10_000);

    // sobe o trabalhador em background para consumir os Jobs
    tokio::spawn(worker(rx, pool.clone()));

    // AppState é clonado para cada request, então conter só o Sender é barato
    let state = AppState { tx };

    // monta a API HTTP com Axum e injeta o estado compartilhado
    let app = Router::new()
        .route("/process", post(ingest))
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:3000".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    println!("Listening on {addr}");

    axum::serve(listener, app).await.unwrap();
}
