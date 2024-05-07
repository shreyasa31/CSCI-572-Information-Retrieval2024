import { Config } from "./src/config";

export const defaultConfig: Config = {
  url: "https://stackoverflow.com/questions",
  match: "https://stackoverflow.com/questions/**",
  maxPagesToCrawl: 50,
  outputFileName: "output.json",
  maxTokens: 2000000,
};
