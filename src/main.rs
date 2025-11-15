// Serviço Axum simples que recebe requisições HTTP e empurra os dados para uma fila
// bounded. Uma única task de background lê a fila em lotes, simulando um trabalho
// lento (gravar no disco, inserir no banco etc.). O objetivo é mostrar como separar
// ingestão rápida de processamento lento mantendo backpressure automático.
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::Deserialize;
use std::{net::SocketAddr, time::Duration};
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
    #[allow(unused)]
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
async fn worker(mut rx: mpsc::Receiver<Job>) {
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

        // aqui seria o "trabalho lento": disco, DB, etc.
        // por enquanto só simula com um log único por lote
        println!("Processando lote de {} itens", buffer.len());
    }
}

/// Inicializa o servidor HTTP e a task de background.
#[tokio::main]
async fn main() {
    // fila bounded para backpressure
    let (tx, rx) = mpsc::channel::<Job>(10_000);

    // sobe o trabalhador em background para consumir os Jobs
    tokio::spawn(worker(rx));

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
