# app/server.py
#
# WHAT IS FLASK?
# Flask is a lightweight Python web framework. It listens for HTTP requests
# and returns responses. When you type a URL in a browser, you're making
# an HTTP GET request. When you submit a form, you're making a POST request.
#
# Our app has three routes:
#   GET  /          → serves the HTML page (the UI)
#   POST /query     → receives a question, returns JSON answer
#   GET  /health    → simple check to confirm the server is running

import os
import sys
import json
from flask import Flask, request, jsonify, render_template_string

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from app.query_engine import answer_question

app = Flask(__name__)

# ── HTML TEMPLATE ──────────────────────────────────────────────────────────────
# We inline the HTML here to keep the app self-contained in one file.
# In a production app this would live in a templates/ folder.
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GTM Analytics — Self-Serve</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #f5f5f5;
            color: #1a1a1a;
            min-height: 100vh;
        }

        header {
            background: #1a1a1a;
            color: white;
            padding: 1.25rem 2rem;
            display: flex;
            align-items: center;
            gap: 12px;
        }

        header h1 { font-size: 1.1rem; font-weight: 500; }
        header span { font-size: 0.85rem; color: #888; }

        .container { max-width: 900px; margin: 2rem auto; padding: 0 1.5rem; }

        /* Query input area */
        .query-box {
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid #e5e5e5;
            margin-bottom: 1.5rem;
        }

        .query-box label {
            display: block;
            font-size: 0.85rem;
            color: #666;
            margin-bottom: 0.5rem;
            font-weight: 500;
        }

        .input-row {
            display: flex;
            gap: 10px;
        }

        .input-row input {
            flex: 1;
            padding: 0.75rem 1rem;
            border: 1px solid #ddd;
            border-radius: 8px;
            font-size: 0.95rem;
            outline: none;
            transition: border-color 0.2s;
        }

        .input-row input:focus { border-color: #1a1a1a; }

        .input-row button {
            padding: 0.75rem 1.5rem;
            background: #1a1a1a;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 0.95rem;
            cursor: pointer;
            transition: opacity 0.2s;
            white-space: nowrap;
        }

        .input-row button:hover { opacity: 0.85; }
        .input-row button:disabled { opacity: 0.5; cursor: not-allowed; }

        /* Example questions */
        .examples {
            margin-top: 1rem;
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }

        .examples span {
            font-size: 0.8rem;
            color: #666;
            margin-right: 4px;
        }

        .example-btn {
            font-size: 0.8rem;
            padding: 4px 10px;
            background: #f5f5f5;
            border: 1px solid #ddd;
            border-radius: 20px;
            cursor: pointer;
            color: #444;
            transition: background 0.15s;
        }

        .example-btn:hover { background: #eee; }

        /* Loading state */
        .loading {
            text-align: center;
            padding: 2rem;
            color: #666;
            font-size: 0.9rem;
            display: none;
        }

        /* Results area */
        .result-card {
            background: white;
            border-radius: 12px;
            border: 1px solid #e5e5e5;
            overflow: hidden;
            margin-bottom: 1rem;
        }

        .result-header {
            padding: 1rem 1.5rem;
            border-bottom: 1px solid #f0f0f0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .result-header h3 { font-size: 0.9rem; font-weight: 500; color: #444; }
        .row-count { font-size: 0.8rem; color: #999; }

        /* Explanation box */
        .explanation {
            padding: 1.25rem 1.5rem;
            font-size: 0.95rem;
            line-height: 1.6;
            color: #1a1a1a;
            background: #fafafa;
            border-bottom: 1px solid #f0f0f0;
        }

        /* SQL box */
        .sql-section {
            padding: 1rem 1.5rem;
            border-bottom: 1px solid #f0f0f0;
        }

        .sql-toggle {
            font-size: 0.8rem;
            color: #888;
            cursor: pointer;
            user-select: none;
        }

        .sql-toggle:hover { color: #444; }

        .sql-code { 
            margin-top: 0.75rem; background: #1a1a1a; color: #e5e5e5; padding: 1rem; border-radius: 6px; font-family: monospace; font-size: 0.8rem; line-height: 1.5; overflow-x: auto; display: none; white-space: pre; scrollbar-color: #555 #1a1a1a; scrollbar-width: thin; }
            .sql-code::-webkit-scrollbar { height: 6px; }
            .sql-code::-webkit-scrollbar-track { background: #1a1a1a; border-radius: 6px; }
            .sql-code::-webkit-scrollbar-thumb { background: #555; border-radius: 6px; }
            .sql-code::-webkit-scrollbar-thumb:hover { background: #888; 
        }

        /* Data table */
        .table-wrapper {
            overflow-x: auto;
            padding: 1rem 1.5rem;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85rem;
        }

        th {
            text-align: left;
            padding: 0.5rem 0.75rem;
            background: #f5f5f5;
            color: #666;
            font-weight: 500;
            border-bottom: 1px solid #eee;
            white-space: nowrap;
        }

        td {
            padding: 0.5rem 0.75rem;
            border-bottom: 1px solid #f5f5f5;
            color: #1a1a1a;
        }

        tr:last-child td { border-bottom: none; }
        tr:hover td { background: #fafafa; }

        /* Error state */
        .error-card {
            background: #fff5f5;
            border: 1px solid #fecaca;
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
            color: #dc2626;
            font-size: 0.9rem;
        }
    </style>
</head>
<body>

<header>
    <h1>GTM Analytics</h1>
    <span>Self-serve query tool</span>
</header>

<div class="container">

    <div class="query-box">
        <label>Ask a question about your GTM data</label>
        <div class="input-row">
            <input
                type="text"
                id="question"
                placeholder="e.g. What is our pipeline coverage by region?"
                onkeydown="if(event.key==='Enter') runQuery()"
            />
            <button id="ask-btn" onclick="runQuery()">Ask</button>
        </div>

        <div class="examples">
            <span>Try:</span>
            <button class="example-btn" onclick="ask('What is our MRR by region?')">MRR by region</button>
            <button class="example-btn" onclick="ask('Which accounts have the highest upsell potential?')">Upsell targets</button>
            <button class="example-btn" onclick="ask('What is our win rate by region?')">Win rate</button>
            <button class="example-btn" onclick="ask('Show me open pipeline by stage in EMEA')">EMEA pipeline</button>
            <button class="example-btn" onclick="ask('Which industry has the highest average deal size?')">Deal size by industry</button>
        </div>
    </div>

    <div class="loading" id="loading">
        Analysing your question and querying the warehouse...
    </div>

    <div id="results"></div>

</div>

<script>
    function ask(question) {
        document.getElementById('question').value = question;
        runQuery();
    }

    async function runQuery() {
        const question = document.getElementById('question').value.trim();
        if (!question) return;

        // Show loading state
        document.getElementById('loading').style.display = 'block';
        document.getElementById('results').innerHTML = '';
        document.getElementById('ask-btn').disabled = true;

        try {
            // POST the question to our Flask /query endpoint
            const response = await fetch('/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question })
            });

            const data = await response.json();
            renderResults(data);

        } catch (err) {
            document.getElementById('results').innerHTML =
                `<div class="error-card">Network error: ${err.message}</div>`;
        } finally {
            document.getElementById('loading').style.display = 'none';
            document.getElementById('ask-btn').disabled = false;
        }
    }

    function renderResults(data) {
        const container = document.getElementById('results');

        if (data.error && !data.sql) {
            container.innerHTML = `<div class="error-card">${data.error}</div>`;
            return;
        }

        let html = `<div class="result-card">`;

        // Header
        html += `<div class="result-header">
            <h3>${escapeHtml(data.question)}</h3>
            ${data.row_count !== undefined ? `<span class="row-count">${data.row_count} rows</span>` : ''}
        </div>`;

        // Plain English explanation
        if (data.explanation) {
            html += `<div class="explanation">${escapeHtml(data.explanation)}</div>`;
        }

        // SQL (collapsible)
        if (data.sql) {
            html += `<div class="sql-section">
                <span class="sql-toggle" onclick="toggleSql(this)">Show SQL query ▾</span>
                <pre class="sql-code">${escapeHtml(data.sql)}</pre>
            </div>`;
        }

        // Error on SQL execution
        if (data.error) {
            html += `<div style="padding: 1rem 1.5rem; color: #dc2626; font-size: 0.85rem;">
                ${escapeHtml(data.error)}
            </div>`;
        }

        // Data table
        if (data.columns && data.columns.length > 0 && data.data.length > 0) {
            html += `<div class="table-wrapper"><table>`;

            // Header row
            html += `<tr>` + data.columns.map(c =>
                `<th>${escapeHtml(c)}</th>`
            ).join('') + `</tr>`;

            // Data rows
            data.data.forEach(row => {
                html += `<tr>` + data.columns.map(c => {
                    const val = row[c];
                    // Format numbers nicely
                    const display = typeof val === 'number'
                        ? val.toLocaleString('en-US', { maximumFractionDigits: 2 })
                        : (val === null ? '—' : escapeHtml(String(val)));
                    return `<td>${display}</td>`;
                }).join('') + `</tr>`;
            });

            html += `</table></div>`;
        }

        html += `</div>`;
        container.innerHTML = html;
    }

    function toggleSql(el) {
        const code = el.nextElementSibling;
        const isVisible = code.style.display === 'block';
        code.style.display = isVisible ? 'none' : 'block';
        el.textContent = isVisible ? 'Show SQL query ▾' : 'Hide SQL query ▴';
    }

    function escapeHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }
</script>

</body>
</html>
"""


@app.route("/")
def index():
    """Serve the main UI page."""
    return render_template_string(HTML)


@app.route("/query", methods=["POST"])
def query():
    """
    Receive a natural language question, return a JSON answer.

    The frontend POSTs { "question": "..." } here.
    We call answer_question() and return the result as JSON.
    """
    data = request.get_json()
    question = data.get("question", "").strip()

    if not question:
        return jsonify({"error": "No question provided"}), 400

    result = answer_question(question)
    return jsonify(result)


@app.route("/health")
def health():
    """Simple health check endpoint."""
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print("\nGTM Analytics Self-Serve Tool")
    print("Open http://localhost:5000 in your browser\n")
    # debug=True auto-reloads when you save changes — great for development
    app.run(debug=True, port=5000)