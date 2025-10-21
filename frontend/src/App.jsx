// import { useState } from 'react'
// import reactLogo from './assets/react.svg'
// import viteLogo from '/vite.svg'
// import './App.css'

// function App() {
//   const [count, setCount] = useState(0)

//   return (
//     <>
//       <div>
//         <a href="https://vite.dev" target="_blank">
//           <img src={viteLogo} className="logo" alt="Vite logo" />
//         </a>
//         <a href="https://react.dev" target="_blank">
//           <img src={reactLogo} className="logo react" alt="React logo" />
//         </a>
//       </div>
//       <h1>Vite + React</h1>
//       <div className="card">
//         <button onClick={() => setCount((count) => count + 1)}>
//           count is {count}
//         </button>
//         <p>
//           Edit <code>src/App.jsx</code> and save to test HMR
//         </p>
//       </div>
//       <p className="read-the-docs">
//         Click on the Vite and React logos to learn more
//       </p>
//     </>
//   )
// }

// export default App

import { useEffect, useState } from "react";

function App() {
  const [pipelines, setPipelines] = useState([]);
  const [selectedPipeline, setSelectedPipeline] = useState(null);
  const [expandedStages, setExpandedStages] = useState({});

  // Fetch pipelines from the backend
  useEffect(() => {
    fetch("/api/pipelines")
      .then((res) => res.json())
      .then((data) => {
        // Flatten pipelines from each config object
        const allPipelines = data.flatMap((config) => config.pipelines);
        setPipelines(allPipelines);
      })
      .catch((err) => console.error("Error fetching pipelines:", err));
  }, []);

  // Toggle stage details
  const toggleStage = (stageId) => {
    setExpandedStages((prev) => ({
      ...prev,
      [stageId]: !prev[stageId],
    }));
  };

  return (
    <div style={{ padding: "2rem", fontFamily: "Arial, sans-serif" }}>
      <h1>Data Pipelines</h1>

      {/* Pipeline dropdown */}
      <select
        onChange={(e) =>
          setSelectedPipeline(
            pipelines.find((p) => p.pipeline_id === e.target.value)
          )
        }
        value={selectedPipeline?.pipeline_id || ""}
        style={{ padding: "0.5rem", fontSize: "1rem" }}
      >
        <option value="">Select a pipeline</option>
        {pipelines.map((p) => (
          <option key={p.pipeline_id} value={p.pipeline_id}>
            {p.pipeline_name}
          </option>
        ))}
      </select>

      {/* Selected pipeline details */}
      {selectedPipeline && (
        <div style={{ marginTop: "2rem" }}>
          <h2>{selectedPipeline.pipeline_name}</h2>
          <p>{selectedPipeline.description}</p>
          <p>
            <strong>Last run:</strong>{" "}
            {new Date(selectedPipeline.last_run).toLocaleString()}
          </p>
          <p>
            <strong>Final output:</strong>{" "}
            {selectedPipeline.final_output?.database_table ||
              "Not specified"}{" "}
            ({selectedPipeline.final_output?.record_count || 0} records)
          </p>

          <h3>Stages</h3>
          <ul style={{ listStyle: "none", paddingLeft: 0 }}>
            {selectedPipeline.stages.map((stage) => (
              <li
                key={stage.stage_id}
                style={{
                  marginBottom: "1rem",
                  border: "1px solid #ccc",
                  padding: "1rem",
                  borderRadius: "8px",
                  cursor: "pointer",
                  backgroundColor: "#f9f9f9",
                }}
                onClick={() => toggleStage(stage.stage_id)}
              >
                <strong>
                  {stage.stage_number}. {stage.stage_name || "Unnamed Stage"} (
                  {stage.stage_type || "N/A"})
                </strong>

                {/* Stage details (expand/collapse) */}
                {expandedStages[stage.stage_id] && (
                  <div style={{ marginTop: "0.5rem" }}>
                    <p>
                      <strong>Description:</strong> {stage.description}
                    </p>
                    {stage.notes && (
                      <p>
                        <strong>Notes:</strong> {stage.notes}
                      </p>
                    )}
                    <p>
                      <strong>Execution time:</strong> {stage.execution_time_ms}{" "}
                      ms
                    </p>
                    {stage.code_snippet && (
                      <pre
                        style={{
                          backgroundColor: "#eee",
                          padding: "0.5rem",
                          overflowX: "auto",
                        }}
                      >
                        {stage.code_snippet}
                      </pre>
                    )}
                  </div>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

export default App;
