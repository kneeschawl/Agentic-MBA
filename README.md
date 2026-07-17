# Agentic-MBA: Agentic Market Basket Analyzer 🚀

Agentic-MBA is a data-agnostic, event-driven web application that bridges traditional data mining with local AI orchestration. The platform utilizes an **FP-Growth engine** to dynamically extract high-signal product association rules from raw transaction datasets. A team of specialized, local **Llama-3.1 agents** then automatically translates those complex mathematical metrics (Support, Confidence, and Lift) into actionable consumer psychology profiles and clear physical or digital merchandising strategies.

Built on a non-blocking WebSocket architecture, the system provides real-time processing logs via a live terminal interface, maps connections dynamically onto an interactive **Visual Association Map**, and delivers an executive boardroom-ready report downloadable as a print-formatted PDF in a single click.

---

## 🌟 Key Features

* **Data-Agnostic FP-Growth Mining:** Ingests raw transactional CSV data on the fly, dynamically generating association rules without hardcoded dependencies.
* **Multi-Agent AI Pipeline:** Orchestrates local Llama-3.1 models acting as specialized business agents (Consumer Psychologist, Merchandising Strategist) to draft qualitative retail advice.
* **Event-Driven UI Stream:** Utilizes high-performance WebSockets to pipe real-time execution milestones directly into a client-side terminal interface with zero UI freezing.
* **Interactive Network Graph:** Dynamically visualizes product affinities, tracking how different product categories structurally pull and cluster together.
* **Boardroom-Ready Reports:** Generates executive briefs containing strategic actions, psychological triggers, and estimated financial impacts (AOV / retention metrics) with native PDF export functionality.

---

## 🛠️ Core Stack

* **Backend & AI:** Python, FastAPI, WebSockets, Llama-3.1 (Local Inference)
* **Data Mining:** FP-Growth (Association Rules via Pandas/Mlxtend)
* **Frontend:** JavaScript (ES6+), Tailwind CSS, Interactive Network Graphs, html2pdf.js

---

## 📂 Project Architecture

```text
[ User Browser ]
       │
       ├─── (Static Assets & Layout) ────────> Tailwind UI + D3/Vis.js Graph
       │
       └─── (WSS: Live Event Stream) ────────> FastAPI Backend 
                                                     │
                                                     ├──> FP-Growth Engine
                                                     └──> Multi-Agent Llama Pipeline