module.exports = {
  apps: [
    {
      name: "jaeger",
      script: "/opt/apps/jaeger/jaeger-all-in-one",
    },
    {
      name: "maelstrom",
      script: "/opt/apps/maelstrom/maelstrom serve",
    },
  ],
};
