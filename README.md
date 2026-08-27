# Agentic-MBA: Agentic Market Basket Analyzer 🚀

Agentic-MBA is a data-agnostic, event-driven web application that bridges traditional data mining with local AI orchestration. The platform utilizes an **FP-Growth engine** to dynamically extract high-signal product association rules from raw transaction datasets. A team of specialized, local **Llama-3.1 agents** then automatically translates those complex mathematical metrics (Support, Confidence, and Lift) into actionable consumer psychology profiles and clear physical or digital merchandising strategies.
Agentic-MBA is an event-driven analytics app that combines **FP-Growth association mining** with a **local multi-agent LLM pipeline** to turn raw transaction data into actionable retail strategy.

Built on a non-blocking WebSocket architecture, the system provides real-time processing logs via a live terminal interface, maps connections dynamically onto an interactive **Visual Association Map**, and delivers an executive boardroom-ready report downloadable as a print-formatted PDF in a single click.
It accepts a CSV of transaction line items, computes support/confidence/lift rules, streams progress in real time over WebSockets, visualizes product affinity relationships, and generates an executive-ready strategy brief that can be exported as PDF.

---

## 🌟 Key Features
## What this project does

* **Data-Agnostic FP-Growth Mining:** Ingests raw transactional CSV data on the fly, dynamically generating association rules without hardcoded dependencies.
* **Multi-Agent AI Pipeline:** Orchestrates local Llama-3.1 models acting as specialized business agents (Consumer Psychologist, Merchandising Strategist) to draft qualitative retail advice.
* **Event-Driven UI Stream:** Utilizes high-performance WebSockets to pipe real-time execution milestones directly into a client-side terminal interface with zero UI freezing.
* **Interactive Network Graph:** Dynamically visualizes product affinities, tracking how different product categories structurally pull and cluster together.
* **Boardroom-Ready Reports:** Generates executive briefs containing strategic actions, psychological triggers, and estimated financial impacts (AOV / retention metrics) with native PDF export functionality.
- Mines frequent itemsets and association rules from transactional data.
- Ranks and selects top rules for downstream AI interpretation.
- Uses local Ollama-hosted `llama3.1:8b` agents for:
  - Consumer psychology interpretation
  - Visual merchandising recommendations
  - Executive report synthesis
- Streams live task status from backend worker to browser UI.
- Renders:
  - Rule table (support/confidence/lift)
  - Interactive association graph
  - Markdown executive brief (with PDF export)

---

## 🛠️ Core Stack
## Architecture overview

* **Backend & AI:** Python, FastAPI, WebSockets, Llama-3.1 (Local Inference)
* **Data Mining:** FP-Growth (Association Rules via Pandas/Mlxtend)
* **Frontend:** JavaScript (ES6+), Tailwind CSS, Interactive Network Graphs, html2pdf.js
```text
Browser UI
  ├─ Upload CSV -> POST /upload-csv
  ├─ Open WebSocket -> /ws/{job_id}
  └─ Render progress + final payload
FastAPI Gateway (main.py)
  ├─ Stores upload in staged_datasets/
  ├─ Enqueues Celery task (task_id = job_id)
  └─ Exposes WebSocket polling Celery task state
Celery Worker (tasks.py)
  ├─ Load CSV with pandas
  ├─ Run FP-Growth + association_rules (mlxtend)
  ├─ Run agent chain (agents.py via Ollama HTTP API)
  └─ Return merged payload
Redis
  └─ Celery broker + result backend
Ollama (local)
  └─ Model: llama3.1:8b
```

---

## 📂 Project Architecture
## Tech stack

```text
[ User Browser ]
       │
       ├─── (Static Assets & Layout) ────────> Tailwind UI + D3/Vis.js Graph
       │
       └─── (WSS: Live Event Stream) ────────> FastAPI Backend 
                                                     │
                                                     ├──> FP-Growth Engine
                                                     └──> Multi-Agent Llama Pipeline
- **Backend:** FastAPI, Uvicorn
- **Async jobs:** Celery + Redis
- **Data mining:** pandas, mlxtend (FP-Growth + association rules)
- **Agent orchestration:** httpx + Ollama local API
- **Frontend:** server-rendered HTML + Tailwind + vis-network + marked.js + html2pdf.js

---

## Prerequisites

1. **Python** 3.10+ (recommended: 3.11)
2. **Redis** running on `127.0.0.1:6379`
3. **Ollama** installed and running locally
4. Ollama model available:

```bash
ollama pull llama3.1:8b
```

---

## Installation

```bash
# from repository root
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Run the system

Start services in separate terminals.

### 1) Start Redis

```bash
redis-server
```

### 2) Start Celery worker

```bash
celery -A tasks.celery_app worker --pool=solo --loglevel=info
```

### 3) Start FastAPI app

```bash
uvicorn main:app --reload
```

Then open:

- `http://127.0.0.1:8000`

---

## Input data format

Upload a CSV containing at least these columns:

- `TransactionID`
- `Item`

Example:

```csv
TransactionID,Item
1001,Bread
1001,Butter
1002,Milk
1002,Cereal
1002,Banana
```

---

## API + runtime behavior

### `POST /upload-csv`
- Accepts multipart file upload.
- Stores file under `staged_datasets/`.
- Enqueues `tasks.process_market_basket` Celery task.
- Returns `{ "status": "queued", "job_id": "..." }`.

### `WS /ws/{job_id}`
- Polls Celery task state and streams events:
  - `progress` -> status text
  - `complete` -> final analytics + agent output payload
  - `error` -> failure status

### `GET /`
- Serves the dashboard UI.

---

## Output payload (high level)

Final completion payload includes:

- `rules`: list of discovered association rules with
  - `antecedents`
  - `consequents`
  - `support`
  - `confidence`
  - `lift`
- `psychology`: agent-generated behavioral insights
- `merchandising`: agent-generated placement strategies
- `report`: executive markdown brief

---

## Project files

- `/home/runner/work/Agentic-MBA/Agentic-MBA/main.py` - FastAPI app + UI + websocket stream
- `/home/runner/work/Agentic-MBA/Agentic-MBA/tasks.py` - Celery task pipeline and FP-Growth execution
- `/home/runner/work/Agentic-MBA/Agentic-MBA/agents.py` - Ollama prompts and async agent runners
- `/home/runner/work/Agentic-MBA/Agentic-MBA/schemas.py` - Pydantic output schemas
- `/home/runner/work/Agentic-MBA/Agentic-MBA/celery_config.py` - Celery/Redis settings
- `/home/runner/work/Agentic-MBA/Agentic-MBA/execution_commands.txt` - convenience run commands

---

## Notes and limitations

- Ollama endpoint is hardcoded to `http://localhost:11434/api/chat`.
- Default model name is hardcoded as `llama3.1:8b`.
- FP-Growth support threshold is currently `0.2` and rule lift threshold is `1.0`.
- If no frequent itemsets are found, the system returns an empty strategy payload with a fallback report message.

---

## License

This repository includes a [LICENSE](./LICENSE) file. Refer to it for usage terms.
