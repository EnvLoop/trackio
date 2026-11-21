import { mount } from "svelte";
import "./lib/gradio-theme.css";
import EmbedDashboard from "./EmbedDashboard.svelte";

if (window.__trackio_embed === true) {
  mount(EmbedDashboard, { target: document.getElementById("app") });
}
