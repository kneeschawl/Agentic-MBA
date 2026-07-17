# main.py
import os
import asyncio
import uuid
import shutil
from fastapi import FastAPI, UploadFile, File, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import HTMLResponse
from typing import Dict
from tasks import process_market_basket, celery_app

app = FastAPI(title="Agentic-MBA Event-Driven Gateway Engine")

# Tracks active live-websocket nodes mapped to specific transaction IDs
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, job_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[job_id] = websocket
        print(f"[Gateway] Open live WebSocket pipe for target stream: {job_id}")

    def disconnect(self, job_id: str):
        if job_id in self.active_connections:
            del self.active_connections[job_id]
            print(f"[Gateway] Terminated active pipe context: {job_id}")

    async def send_json_payload(self, job_id: str, data: dict):
        if job_id in self.active_connections:
            await self.active_connections[job_id].send_json(data)
            print(f"[Gateway] Data broadcast complete for context channel: {job_id}")

manager = ConnectionManager()

# Create a dedicated local folder to securely stage analytical CSVs
UPLOAD_DIR = "staged_datasets"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Accepts the streaming data file, stages it locally to prevent RAM bloat,
    and forwards the localized path string to the Celery worker cluster.
    """
    try:
        # 1. Generate a completely unique, thread-safe tracking key
        job_id = str(uuid.uuid4())
        
        # 2. Define the exact isolated file track path
        file_extension = os.path.splitext(file.filename)[1]
        staged_file_path = os.path.join(UPLOAD_DIR, f"{job_id}{file_extension}")
        
        # 3. Stream incoming chunks directly to disk safely
        with open(staged_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # 4. --- FIXED: Force Celery to use our custom tracking job_id ---
        process_market_basket.apply_async(args=[staged_file_path], task_id=job_id)
        
        return {"status": "queued", "job_id": job_id}
        
    except Exception as e:
        return {"status": "error", "message": f"Staging failed: {str(e)}"}

@app.post("/webhook/job-complete/{job_id}")
async def receive_worker_results(job_id: str, payload: dict):
    """Acts as internal hook listener to receive data packages from Celery workers"""
    print(f"[Gateway Webhook] Received completion packet from background worker for {job_id}")
    await manager.send_json_payload(job_id, payload)
    return {"message": "Broadcast executed down socket channels successfully."}

@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await websocket.accept()
    
    # Check our Celery app instance from tasks
    from tasks import celery_app
    task_result = celery_app.AsyncResult(job_id)
    
    last_status = None
    
    try:
        while True:
            # Refresh the task state from Redis
            state = task_result.state
            
            if state == 'PROGRESS':
                # Grab our custom status message
                meta = task_result.info or {}
                status_msg = meta.get('status_msg', 'Processing...')
                
                # Send the update if the message has changed
                if status_msg != last_status:
                    await websocket.send_json({
                        "event": "progress",
                        "message": status_msg
                    })
                    last_status = status_msg
                    
            elif state == 'SUCCESS':
                # Task finished successfully! Grab final output payload
                final_payload = task_result.result
                await websocket.send_json({
                    "event": "complete",
                    "payload": final_payload
                })
                break
                
            elif state in ['FAILURE', 'REVOKED']:
                await websocket.send_json({
                    "event": "error",
                    "message": f"Task execution halted with state: {state}"
                })
                break
                
            # Poll every 500ms to keep it ultra-responsive
            await asyncio.sleep(0.5)
            
    except WebSocketDisconnect:
        print(f"Websocket connection closed by client for job {job_id}")
@app.get("/", response_class=HTMLResponse)
async def get_ui():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Agentic MBA - Executive Dashboard</title>
        <!-- Tailwind CSS for modern layout, marked.js to render Markdown on the fly -->
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
        <!-- Vis.js for Interactive Network Graphs -->
        <link href="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.2/dist/dist/vis-network.min.css" rel="stylesheet" type="text/css" />
        <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.2/dist/vis-network.min.js"></script>
        <!-- html2pdf.js Library -->
        <script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js"></script>
        <style>
            /* Custom Scrollbar */
            ::-webkit-scrollbar { width: 6px; height: 6px; }
            ::-webkit-scrollbar-track { background: #f1f5f9; }
            ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }
            ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
            
            /* Professional typography inside the generated report */
            .prose h1 { font-size: 1.8rem; font-weight: 800; color: #1e293b; margin-top: 1.5rem; margin-bottom: 0.75rem; border-bottom: 2px solid #e2e8f0; padding-bottom: 0.5rem; }
            .prose h2 { font-size: 1.4rem; font-weight: 700; color: #334155; margin-top: 1.25rem; margin-bottom: 0.5rem; }
            .prose h3 { font-size: 1.1rem; font-weight: 600; color: #475569; margin-top: 1rem; margin-bottom: 0.5rem; }
            .prose p { color: #475569; margin-bottom: 0.75rem; line-height: 1.6; }
            .prose table { width: 100%; border-collapse: collapse; margin: 1rem 0; font-size: 0.9rem; }
            .prose th { background-color: #f8fafc; color: #1e293b; font-weight: 600; text-align: left; padding: 10px; border: 1px solid #e2e8f0; }
            .prose td { padding: 10px; border: 1px solid #e2e8f0; color: #334155; vertical-align: top; }
            .prose tr:nth-child(even) { background-color: #f8fafc; }
        </style>
    </head>
    <body class="bg-slate-50 min-h-screen flex flex-col font-sans">

        <!-- Top Header -->
        <header class="bg-white border-b border-slate-200 px-8 py-4 flex items-center justify-between shadow-sm">
            <div class="flex items-center gap-3">
                <div class="bg-indigo-600 text-white p-2 rounded-lg font-bold text-lg tracking-wider">MBA</div>
                <div>
                    <h1 class="font-bold text-slate-800 text-lg leading-tight">Agentic Market Basket Analyzer</h1>
                    <p class="text-xs text-slate-500">FP-Growth Engine + Local Llama-3.1 Orchestration</p>
                </div>
            </div>
            
            <div class="flex items-center gap-4">
                <input type="file" id="csvFileInput" accept=".csv" class="hidden" onchange="updateFileName()" />
                <label for="csvFileInput" class="cursor-pointer bg-slate-100 hover:bg-slate-200 text-slate-700 px-4 py-2 rounded-lg text-sm font-semibold transition border border-slate-300 flex items-center gap-2">
                    <span id="fileNameLabel">Choose Transactions CSV</span>
                </label>
                <button onclick="triggerJob()" class="bg-indigo-600 hover:bg-indigo-700 text-white px-5 py-2 rounded-lg text-sm font-semibold transition shadow-sm flex items-center gap-2">
                    ⚡ Analyze Dataset
                </button>
            </div>
        </header>

        <!-- Main Workspace (Grid layout) - Now using 'md' for side-by-side rendering -->
        <main class="flex-1 grid grid-cols-1 md:grid-cols-12 gap-6 p-6 overflow-hidden max-w-[1600px] mx-auto w-full">
            
            <!-- LEFT COLUMN (5 of 12 Cols): Status, Rules, and Network Graph -->
            <section class="md:col-span-5 flex flex-col gap-6 h-[calc(100vh-140px)] min-h-[500px]">
                
                <!-- Status Card -->
                <div class="bg-white rounded-xl border border-slate-200 p-5 shadow-sm shrink-0">
                    <h3 class="text-xs font-bold uppercase tracking-wider text-slate-400 mb-2">System Terminal</h3>
                    <div class="flex items-center gap-3">
                        <span class="relative flex h-3 w-3">
                            <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                            <span class="relative inline-flex rounded-full h-3 w-3 bg-emerald-500" id="statusIndicator"></span>
                        </span>
                        <span class="font-semibold text-slate-700 text-sm" id="statusText">System Idle - Waiting for CSV Upload</span>
                    </div>
                </div>

                <!-- Rules Grid Table Card -->
                <div class="bg-white rounded-xl border border-slate-200 shadow-sm flex-1 flex flex-col overflow-hidden">
                    <div class="p-5 border-b border-slate-100 shrink-0">
                        <h2 class="font-bold text-slate-800 text-base">Mathematical Association Rules</h2>
                        <p class="text-xs text-slate-400">Rules generated directly by FP-Growth</p>
                    </div>
                    
                    <div class="overflow-y-auto flex-1" id="rulesTableContainer">
                        <div class="text-center py-12 text-slate-400 text-sm">
                            No rules calculated yet. Upload a dataset to begin.
                        </div>
                    </div>
                </div>
                
                <!-- Network Graph Visualizer Panel -->
                <div class="bg-white p-5 rounded-xl border border-slate-200 shadow-sm flex-1 flex flex-col overflow-hidden">
                    <div class="mb-4 shrink-0">
                        <div class="flex items-center justify-between">
                            <h2 class="font-bold text-slate-800 text-base flex items-center gap-2">
                                🕸️ Visual Association Map
                            </h2>
                            <span class="text-xs text-slate-400 font-medium">Drag to explore / Scroll to zoom</span>
                        </div>
                        <p class="text-xs text-slate-400 mt-1">Interactive layout of product connections</p>
                    </div>
                    <!-- The Graph Container -->
                    <div id="networkGraph" class="w-full flex-1 bg-slate-50 rounded-lg border border-slate-100 relative overflow-hidden">
                        <div id="graphPlaceholder" class="absolute inset-0 flex items-center justify-center text-xs text-slate-400 font-mono">
                            Awaiting dataset analysis to map associations...
                        </div>
                    </div>
                </div>

            </section>

            <!-- RIGHT COLUMN (7 of 12 Cols): The Agentic Executive Report -->
            <section class="md:col-span-7 bg-white rounded-xl border border-slate-200 shadow-sm flex flex-col h-[calc(100vh-140px)] overflow-hidden">
                <div class="p-6 border-b border-slate-100 flex items-center justify-between shrink-0">
                    <div>
                        <h2 class="font-bold text-slate-800 text-lg">Agentic Executive Brief</h2>
                        <p class="text-xs text-slate-400">AI-generated strategy and business recommendations</p>
                    </div>
                    <div class="flex items-center gap-3">
                        <span id="reportBadge" class="hidden px-2.5 py-1 text-xs font-semibold rounded-full bg-indigo-50 text-indigo-600 border border-indigo-100 animate-pulse">
                            Draft Ready
                        </span>
                        <!-- The PDF Export Button (Starts hidden via Tailwind 'hidden') -->
                        <button onclick="exportReportToPDF()" id="exportPdfBtn" class="hidden bg-slate-800 hover:bg-slate-900 text-white px-3.5 py-1.5 rounded-lg text-xs font-semibold transition shadow-sm items-center gap-1.5">
                            📥 Export PDF
                        </button>
                    </div>
                </div>
                
                <!-- Report Content Area -->
                <div class="p-6 overflow-y-auto flex-1 prose prose-slate max-w-none" id="reportContent">
                    <div class="text-center py-24 text-slate-400">
                        <p class="text-sm font-mono">Awaiting pipeline resolution to generate executive strategy...</p>
                    </div>
                </div>
            </section>

        </main>

        <script>
            function updateFileName() {
                const fileInput = document.getElementById('csvFileInput');
                const label = document.getElementById('fileNameLabel');
                if (fileInput.files.length > 0) {
                    label.innerText = fileInput.files[0].name;
                    label.classList.add("bg-indigo-50", "text-indigo-700", "border-indigo-300");
                }
            }

            function triggerJob() {
                const fileInput = document.getElementById('csvFileInput');
                const file = fileInput.files[0];
                
                if (!file) {
                    alert("Please choose a valid transaction .csv file first!");
                    return;
                }

                const statusIndicator = document.getElementById('statusIndicator');
                const systemTerminal = document.getElementById('statusText');
                const rulesTableContainer = document.getElementById('rulesTableContainer');
                const reportContent = document.getElementById('reportContent');
                
                // Set Up Live Terminal Log Viewport
                systemTerminal.innerHTML = `<div class="font-mono text-xs text-slate-500 space-y-1 h-[60px] overflow-y-auto" id="logLines"></div>`;
                const logLines = document.getElementById('logLines');
                
                function appendLog(msg, isHeader=false) {
                    const time = new Date().toLocaleTimeString();
                    const colorClass = isHeader ? "text-indigo-600 font-bold" : "text-slate-600";
                    logLines.innerHTML += `<div class="${colorClass}">[${time}] ${msg}</div>`;
                    logLines.scrollTop = logLines.scrollHeight;
                }
                
                statusIndicator.className = "relative inline-flex rounded-full h-3 w-3 bg-amber-500 animate-pulse";
                appendLog("Staging dataset in background queue...", true);
                
                const formData = new FormData();
                formData.append("file", file);

                fetch('/upload-csv', { method: 'POST', body: formData })
                .then(res => {
                    if (!res.ok) throw new Error("Server HTTP Error: " + res.status);
                    return res.json();
                })
                .then(data => {
                    const jobId = data.job_id;
                    appendLog("Task submitted successfully. Connecting live websocket stream...", false);
                    
                    const ws = new WebSocket("ws://127.0.0.1:8000/ws/" + jobId);
                    
                    ws.onmessage = function(event) {
                        const payload = JSON.parse(event.data);
                        
                        if (payload.event === "progress") {
                            appendLog(payload.message);
                        } 
                        else if (payload.event === "complete") {
                            appendLog("All pipelines successfully resolved!", true);
                            statusIndicator.className = "relative inline-flex rounded-full h-3 w-3 bg-emerald-500";
                            
                            // 1. Render traditional rules table
                            renderRulesTable(payload.payload.rules);
                            
                            // 2. Render our NEW interactive Network Graph!
                            drawAssociationGraph(payload.payload.rules);
                            
                            // 3. Render Executive Brief Markdown
                            reportContent.innerHTML = marked.parse(payload.payload.report);
                            
                            // 4. Reveal the Draft Ready badge
                            document.getElementById('reportBadge').classList.remove('hidden');

                            // 5. Make the PDF button visible and flex-aligned
                            const exportBtn = document.getElementById('exportPdfBtn');
                            exportBtn.classList.remove('hidden');
                            exportBtn.classList.add('flex');
                            
                            ws.close();
                        }
                        else if (payload.event === "error") {
                            statusIndicator.className = "relative inline-flex rounded-full h-3 w-3 bg-rose-500";
                            appendLog("CRITICAL ERROR: " + payload.message, true);
                            ws.close();
                        }
                    };
                    
                    ws.onerror = function(err) {
                        statusIndicator.className = "relative inline-flex rounded-full h-3 w-3 bg-rose-500";
                        appendLog("WebSocket connection error.", true);
                        console.error("WS Error: ", err);
                    };
                })
                .catch(err => {
                    statusIndicator.className = "relative inline-flex rounded-full h-3 w-3 bg-rose-500";
                    console.error("Fetch error: ", err);
                });
            }

            function renderRulesTable(rules) {
                const rulesTableContainer = document.getElementById('rulesTableContainer');
                if(!rules || rules.length === 0) {
                    rulesTableContainer.innerHTML = `<div class="text-center py-12 text-slate-400">No rules discovered above support threshold.</div>`;
                    return;
                }

                let html = `
                    <table class="w-full text-left border-collapse">
                        <thead>
                            <tr class="border-b border-slate-100 bg-slate-50/50">
                                <th class="p-4 text-xs font-bold uppercase text-slate-500">Antecedent</th>
                                <th class="p-4 text-xs font-bold uppercase text-slate-500">Consequent</th>
                                <th class="p-4 text-xs font-bold uppercase text-slate-500 text-center">Support</th>
                                <th class="p-4 text-xs font-bold uppercase text-slate-500 text-center">Conf</th>
                                <th class="p-4 text-xs font-bold uppercase text-slate-500 text-center">Lift</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-100">
                `;

                rules.forEach(rule => {
                    html += `
                        <tr class="hover:bg-slate-50/50 transition">
                            <td class="p-4 text-sm font-semibold text-slate-800">${rule.antecedents.join(', ')}</td>
                            <td class="p-4 text-sm font-semibold text-indigo-600">${rule.consequents.join(', ')}</td>
                            <td class="p-4 text-sm text-slate-500 text-center">${Math.round(rule.support * 100)}%</td>
                            <td class="p-4 text-sm text-slate-500 text-center">${Math.round(rule.confidence * 100)}%</td>
                            <td class="p-4 text-sm font-bold text-slate-700 text-center">${rule.lift.toFixed(2)}</td>
                        </tr>
                    `;
                });

                html += `</tbody></table>`;
                rulesTableContainer.innerHTML = html;
            }
            
            function drawAssociationGraph(rules) {
                const container = document.getElementById('networkGraph');
                
                // Hide placeholder
                const placeholder = document.getElementById('graphPlaceholder');
                if (placeholder) placeholder.style.display = 'none';

                // Track unique items to create "Nodes"
                const nodesMap = new Map();
                const edges = [];

                // Process our rules (Antecedent -> Consequent)
                rules.forEach((rule, index) => {
                    const antStr = rule.antecedents.join(" + ");
                    const consStr = rule.consequents.join(" + ");

                    // Add Antecedent Node
                    if (!nodesMap.has(antStr)) {
                        nodesMap.set(antStr, {
                            id: antStr,
                            label: antStr,
                            color: {
                                background: '#e0e7ff', // Soft Indigo
                                border: '#6366f1',
                                highlight: { background: '#818cf8', border: '#4f46e5' }
                            },
                            font: { color: '#312e81', size: 12, face: 'monospace' },
                            shape: 'box',
                            margin: 10
                        });
                    }

                    // Add Consequent Node
                    if (!nodesMap.has(consStr)) {
                        nodesMap.set(consStr, {
                            id: consStr,
                            label: consStr,
                            color: {
                                background: '#ecfdf5', // Soft Mint
                                border: '#10b981',
                                highlight: { background: '#34d399', border: '#059669' }
                            },
                            font: { color: '#064e3b', size: 12, face: 'monospace' },
                            shape: 'box',
                            margin: 10
                        });
                    }

                    // Create a connection (Edge) representing the rule
                    edges.push({
                        from: antStr,
                        to: consStr,
                        label: `Lift: ${rule.lift}`,
                        font: { size: 9, color: '#64748b', align: 'top', face: 'monospace' },
                        arrows: 'to',
                        color: { color: '#cbd5e1', highlight: '#6366f1' },
                        width: Math.min(Math.max(rule.lift * 1.5, 1), 6), // Line thickness represents lift strength!
                        smooth: { type: 'cubicBezier', roundness: 0.5 }
                    });
                });

                const data = {
                    nodes: Array.from(nodesMap.values()),
                    edges: edges
                };

                const options = {
                    physics: {
                        enabled: true,
                        barnesHut: {
                            gravitationalConstant: -2000,
                            centralGravity: 0.3,
                            springLength: 120,
                            springConstant: 0.04
                        }
                    },
                    interaction: {
                        hover: true,
                        zoomView: true,
                        dragView: true
                    }
                };

                // Render the interactive graph!
                new vis.Network(container, data, options);
            }
            
            // Generates a clean A4 print layout from the dashboard card
            function exportReportToPDF() {
                const element = document.getElementById('reportContent');
                
                const opt = {
                    margin:       [15, 15, 15, 15], 
                    filename:     'Agentic_Market_Basket_Executive_Brief.pdf',
                    image:        { type: 'jpeg', quality: 0.98 },
                    html2canvas:  { scale: 2, useCORS: true, letterRendering: true },
                    jsPDF:        { unit: 'mm', format: 'a4', orientation: 'portrait' }
                };

                // Uncap height limits temporarily so the library can capture everything scrolled out of view
                const originalStyle = element.style.maxHeight;
                element.style.maxHeight = 'none'; 

                html2pdf().set(opt).from(element).save().then(() => {
                    element.style.maxHeight = originalStyle; // Restore dashboard UI constraint
                });
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)