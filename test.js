import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 500,
  iterations: 1_000_000,
};

export default function () {
  const url = "http://localhost:3000/process";

  const res = http.post(url, JSON.stringify({ data: "teste" }), {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status 202": (r) => r.status === 202,
  });
}
