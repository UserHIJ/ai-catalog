import Fastify from "fastify";
import dotenv from "dotenv";
dotenv.config();

const app = Fastify({ logger: true });

// health check route
app.get("/health", async () => ({ ok: true, ts: Date.now() }));

// config route, just for fun
app.get("/config", async () => ({
  api: "ok",
  llm: { baseUrl: process.env.LLM_BASE_URL, model: process.env.LLM_MODEL }
}));

const port = Number(process.env.PORT || 3001);
app.listen({ port, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});

