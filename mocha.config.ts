import { MochaOptions } from "mocha";

const config: MochaOptions = {
  require: ["ts-node/register"],
  reporter: "spec",
  ui: "bdd",

  timeout: 10000,
};

export default config;
