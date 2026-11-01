import { fileURLToPath } from "node:url";
import { createSocialMeetServer } from "./app.js";
const port = Number(process.env.PORT || 4011);
const { server } = createSocialMeetServer({
  dataDirectory:
    process.env.DATA_DIRECTORY ||
    fileURLToPath(new URL("./data", import.meta.url)),
  sessionSecret: process.env.SESSION_SECRET,
  ...(process.env.ICE_SERVERS_JSON
    ? { iceServers: JSON.parse(process.env.ICE_SERVERS_JSON) }
    : {}),
});
server.listen(port, "0.0.0.0", () =>
  console.log(`Social Meet API: http://0.0.0.0:${port}`),
);
