import Fastify from "fastify";
import dotenv from "dotenv";
import cors from "@fastify/cors";

dotenv.config();

async function buildServer() {
  const app = Fastify({ logger: true });

  // register CORS so UI (http://localhost:3000) can call API (http://localhost:3001)
  await app.register(cors, { origin: true });

  // health check route
  app.get("/health", async () => ({ ok: true, ts: Date.now() }));

  // config route just to test env passthrough
  app.get("/config", async () => ({
    api: "ok",
    llm: {
      baseUrl: process.env.LLM_BASE_URL,
      model: process.env.LLM_MODEL,
    },
  }));

  const port = Number(process.env.PORT || 3001);
  try {
    await app.listen({ port, host: "0.0.0.0" });
    console.log(`🚀 API running on http://localhost:${port}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

buildServer();

