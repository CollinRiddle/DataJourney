import { useEffect, useState } from "react";

function App() {
  const [pipelines, setPipelines] = useState([]);
  const [selectedPipeline, setSelectedPipeline] = useState(null);
  const [selectedStage, setSelectedStage] = useState(null);
  const [showHome, setShowHome] = useState(true);

  // Fetch pipelines from the backend
  useEffect(() => {
    fetch("/api/pipelines")
      .then((res) => res.json())
      .then((data) => {
        setPipelines(data);
      })
      .catch((err) => console.error("Error fetching pipelines:", err));
  }, []);

  // Stage colors based on type
  const stageColors = {
    data_ingestion: "#10B981",
    data_transformation: "#F59E0B",
    data_loading: "#8B5CF6",
    data_cleaning: "#3B82F6",
  };

  const handleStageClick = (stage) => {
    setSelectedStage(selectedStage?.stage_id === stage.stage_id ? null : stage);
  };

  const handlePipelineSelect = (pipelineId) => {
    const pipeline = pipelines.find((p) => p.pipeline_id === pipelineId);
    setSelectedPipeline(pipeline);
    setSelectedStage(null);
    setShowHome(false);
  };

  return (
    <div style={{
      minHeight: "100vh",
      background: "linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%)",
      color: "#e4e4e7",
      fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    }}>
      {/* Header */}
      <header style={{
        background: "rgba(0, 0, 0, 0.3)",
        backdropFilter: "blur(10px)",
        borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
        padding: "1rem 2rem",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        position: "sticky",
        top: 0,
        zIndex: 100,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
          <h1 style={{
            margin: 0,
            fontSize: "1.75rem",
            background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
            fontWeight: "800",
          }}>
            DataJourney
          </h1>
        </div>
        <button
          onClick={() => setShowHome(true)}
          style={{
            background: "rgba(102, 126, 234, 0.2)",
            border: "1px solid rgba(102, 126, 234, 0.3)",
            color: "#a5b4fc",
            padding: "0.5rem 1.5rem",
            borderRadius: "8px",
            cursor: "pointer",
            fontSize: "0.9rem",
            fontWeight: "600",
            transition: "all 0.3s ease",
          }}
          onMouseEnter={(e) => {
            e.target.style.background = "rgba(102, 126, 234, 0.3)";
            e.target.style.transform = "translateY(-2px)";
          }}
          onMouseLeave={(e) => {
            e.target.style.background = "rgba(102, 126, 234, 0.2)";
            e.target.style.transform = "translateY(0)";
          }}
        >
          🏠 Home
        </button>
      </header>

      <div style={{ display: "flex", minHeight: "calc(100vh - 73px)" }}>
        {/* Sidebar - Pipeline List */}
        <aside style={{
          width: "280px",
          background: "rgba(0, 0, 0, 0.2)",
          backdropFilter: "blur(10px)",
          borderRight: "1px solid rgba(255, 255, 255, 0.1)",
          padding: "1.5rem",
          overflowY: "auto",
        }}>
          <h3 style={{
            fontSize: "0.875rem",
            textTransform: "uppercase",
            letterSpacing: "0.1em",
            color: "#9ca3af",
            marginTop: 0,
            marginBottom: "1rem",
          }}>
            Available Pipelines
          </h3>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
            {pipelines.map((p) => (
              <button
                key={p.pipeline_id}
                onClick={() => handlePipelineSelect(p.pipeline_id)}
                style={{
                  background: selectedPipeline?.pipeline_id === p.pipeline_id
                    ? "linear-gradient(135deg, rgba(102, 126, 234, 0.3), rgba(118, 75, 162, 0.3))"
                    : "rgba(255, 255, 255, 0.05)",
                  border: selectedPipeline?.pipeline_id === p.pipeline_id
                    ? "1px solid rgba(102, 126, 234, 0.5)"
                    : "1px solid rgba(255, 255, 255, 0.1)",
                  color: "#e4e4e7",
                  padding: "1rem",
                  borderRadius: "12px",
                  cursor: "pointer",
                  textAlign: "left",
                  fontSize: "0.95rem",
                  fontWeight: "600",
                  transition: "all 0.3s ease",
                }}
                onMouseEnter={(e) => {
                  if (selectedPipeline?.pipeline_id !== p.pipeline_id) {
                    e.target.style.background = "rgba(255, 255, 255, 0.1)";
                    e.target.style.transform = "translateX(4px)";
                  }
                }}
                onMouseLeave={(e) => {
                  if (selectedPipeline?.pipeline_id !== p.pipeline_id) {
                    e.target.style.background = "rgba(255, 255, 255, 0.05)";
                    e.target.style.transform = "translateX(0)";
                  }
                }}
              >
                <div style={{ marginBottom: "0.25rem" }}>{p.pipeline_name}</div>
                <div style={{ fontSize: "0.75rem", color: "#9ca3af" }}>
                  {p.stages.length} stages
                </div>
              </button>
            ))}
          </div>
        </aside>

        {/* Main Content */}
        <main style={{ flex: 1, padding: "2rem", overflowY: "auto" }}>
          {showHome ? (
            <HomeView />
          ) : selectedPipeline ? (
            <PipelineView
              pipeline={selectedPipeline}
              selectedStage={selectedStage}
              onStageClick={handleStageClick}
              stageColors={stageColors}
            />
          ) : (
            <div style={{ textAlign: "center", marginTop: "4rem" }}>
              <h2 style={{ color: "#9ca3af" }}>Select a pipeline to begin your journey</h2>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

// Home View Component
function HomeView() {
  return (
    <div style={{ maxWidth: "900px", margin: "0 auto" }}>
      <div style={{
        background: "rgba(102, 126, 234, 0.1)",
        border: "1px solid rgba(102, 126, 234, 0.3)",
        borderRadius: "20px",
        padding: "3rem",
        marginBottom: "3rem",
      }}>
        <h1 style={{
          fontSize: "3rem",
          marginTop: 0,
          marginBottom: "1rem",
          background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
          WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent",
        }}>
          Welcome to DataJourney
        </h1>
        <p style={{ fontSize: "1.25rem", color: "#d1d5db", lineHeight: "1.8" }}>
          An interactive exploration platform for understanding data pipelines from start to finish.
        </p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2rem", marginBottom: "3rem" }}>
        <InfoCard
          icon="📊"
          title="Visualize Pipelines"
          description="See the complete data flow with interactive DAG visualizations that bring pipelines to life."
        />
        <InfoCard
          icon="🔍"
          title="Explore Stages"
          description="Click on any stage to dive deep into transformations, code snippets, and execution details."
        />
        <InfoCard
          icon="⚡"
          title="Learn by Doing"
          description="Understand real-world data engineering practices through hands-on exploration."
        />
        <InfoCard
          icon="🎓"
          title="Educational Focus"
          description="Built for students and beginners to demystify the often-overlooked world of data pipelines."
        />
      </div>

      <div style={{
        background: "rgba(0, 0, 0, 0.3)",
        borderRadius: "16px",
        padding: "2rem",
        border: "1px solid rgba(255, 255, 255, 0.1)",
      }}>
        <h2 style={{ marginTop: 0, color: "#f59e0b" }}>Why Data Pipelines Matter</h2>
        <p style={{ lineHeight: "1.8", color: "#d1d5db" }}>
          Data pipelines are the backbone of modern business operations. According to industry research, 
          most data analysis barriers occur during cleaning and merging phases, requiring skilled data 
          engineers to navigate these challenges.
        </p>
        <p style={{ lineHeight: "1.8", color: "#d1d5db" }}>
          The field is rapidly growing, with over 150,000 professionals employed and 20,000+ new jobs 
          added in the past year. DataJourney helps bridge the knowledge gap by making these complex 
          systems accessible and understandable.
        </p>
        <div style={{ marginTop: "1.5rem", fontSize: "0.875rem", color: "#9ca3af" }}>
          <p style={{ margin: "0.5rem 0" }}>
            <strong>Reference:</strong> Pervaiz et al. (2019) - Examining the challenges in development data pipeline
          </p>
          <p style={{ margin: "0.5rem 0" }}>
            <strong>Reference:</strong> 365 Data Science - Data Engineer Job Outlook 2025
          </p>
        </div>
      </div>
    </div>
  );
}

// Info Card Component
function InfoCard({ icon, title, description }) {
  return (
    <div style={{
      background: "rgba(255, 255, 255, 0.05)",
      border: "1px solid rgba(255, 255, 255, 0.1)",
      borderRadius: "16px",
      padding: "1.5rem",
      transition: "all 0.3s ease",
      cursor: "default",
    }}
    onMouseEnter={(e) => {
      e.currentTarget.style.background = "rgba(255, 255, 255, 0.08)";
      e.currentTarget.style.transform = "translateY(-4px)";
      e.currentTarget.style.borderColor = "rgba(102, 126, 234, 0.3)";
    }}
    onMouseLeave={(e) => {
      e.currentTarget.style.background = "rgba(255, 255, 255, 0.05)";
      e.currentTarget.style.transform = "translateY(0)";
      e.currentTarget.style.borderColor = "rgba(255, 255, 255, 0.1)";
    }}>
      <div style={{ fontSize: "2.5rem", marginBottom: "0.5rem" }}>{icon}</div>
      <h3 style={{ margin: "0.5rem 0", color: "#e4e4e7" }}>{title}</h3>
      <p style={{ margin: 0, color: "#9ca3af", lineHeight: "1.6" }}>{description}</p>
    </div>
  );
}

// Pipeline View Component
function PipelineView({ pipeline, selectedStage, onStageClick, stageColors }) {
  return (
    <div>
      {/* Pipeline Header */}
      <div style={{ marginBottom: "2rem" }}>
        <h2 style={{
          fontSize: "2rem",
          marginTop: 0,
          marginBottom: "0.5rem",
          color: "#f3f4f6",
        }}>
          {pipeline.pipeline_name}
        </h2>
        <p style={{ color: "#9ca3af", marginBottom: "1rem" }}>{pipeline.description}</p>
        <div style={{ display: "flex", gap: "2rem", fontSize: "0.9rem" }}>
          <div>
            <span style={{ color: "#9ca3af" }}>Last Run: </span>
            <span style={{ color: "#d1d5db" }}>
              {pipeline.last_run ? new Date(pipeline.last_run).toLocaleString() : "Never"}
            </span>
          </div>
          <div>
            <span style={{ color: "#9ca3af" }}>Output: </span>
            <span style={{ color: "#d1d5db" }}>
              {pipeline.final_output?.database_table || "Not specified"}
            </span>
          </div>
        </div>
      </div>

      {/* DAG Visualization */}
      <div style={{
        background: "rgba(0, 0, 0, 0.3)",
        borderRadius: "20px",
        padding: "3rem 2rem",
        marginBottom: "2rem",
        border: "1px solid rgba(255, 255, 255, 0.1)",
      }}>
        <h3 style={{
          fontSize: "0.875rem",
          textTransform: "uppercase",
          letterSpacing: "0.1em",
          color: "#9ca3af",
          marginTop: 0,
          marginBottom: "2rem",
          textAlign: "center",
        }}>
          Pipeline Flow
        </h3>
        <div style={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          gap: "1rem",
          flexWrap: "wrap",
        }}>
          {pipeline.stages.map((stage, index) => (
            <div key={stage.stage_id} style={{ display: "flex", alignItems: "center" }}>
              {/* Stage Node */}
              <div
                onClick={() => onStageClick(stage)}
                style={{
                  background: selectedStage?.stage_id === stage.stage_id
                    ? `linear-gradient(135deg, ${stageColors[stage.stage_type] || "#6b7280"}dd, ${stageColors[stage.stage_type] || "#6b7280"})`
                    : `${stageColors[stage.stage_type] || "#6b7280"}33`,
                  border: `2px solid ${stageColors[stage.stage_type] || "#6b7280"}`,
                  borderRadius: "16px",
                  padding: "1.5rem",
                  minWidth: "180px",
                  cursor: "pointer",
                  transition: "all 0.3s ease",
                  position: "relative",
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.transform = "scale(1.05) translateY(-4px)";
                  e.currentTarget.style.boxShadow = `0 10px 30px ${stageColors[stage.stage_type] || "#6b7280"}50`;
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.transform = "scale(1) translateY(0)";
                  e.currentTarget.style.boxShadow = "none";
                }}
              >
                <div style={{
                  position: "absolute",
                  top: "-12px",
                  left: "12px",
                  background: stageColors[stage.stage_type] || "#6b7280",
                  color: "#fff",
                  fontSize: "0.75rem",
                  fontWeight: "700",
                  padding: "0.25rem 0.75rem",
                  borderRadius: "12px",
                }}>
                  Stage {stage.stage_number}
                </div>
                <div style={{
                  fontSize: "1rem",
                  fontWeight: "600",
                  color: "#f3f4f6",
                  marginTop: "0.5rem",
                  marginBottom: "0.5rem",
                }}>
                  {stage.stage_name}
                </div>
                <div style={{
                  fontSize: "0.75rem",
                  color: "#9ca3af",
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                }}>
                  {stage.stage_type.replace(/_/g, " ")}
                </div>
              </div>

              {/* Arrow */}
              {index < pipeline.stages.length - 1 && (
                <div style={{
                  color: "#4b5563",
                  fontSize: "2rem",
                  margin: "0 0.5rem",
                }}>
                  →
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Stage Details Panel */}
      {selectedStage && (
        <div style={{
          background: "rgba(0, 0, 0, 0.4)",
          borderRadius: "20px",
          padding: "2rem",
          border: `2px solid ${stageColors[selectedStage.stage_type] || "#6b7280"}`,
          animation: "slideIn 0.3s ease",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "start", marginBottom: "1.5rem" }}>
            <div>
              <h3 style={{
                margin: 0,
                marginBottom: "0.5rem",
                fontSize: "1.5rem",
                color: stageColors[selectedStage.stage_type] || "#6b7280",
              }}>
                {selectedStage.stage_name}
              </h3>
              <p style={{ margin: 0, color: "#d1d5db" }}>{selectedStage.description}</p>
            </div>
            <button
              onClick={() => onStageClick(selectedStage)}
              style={{
                background: "rgba(255, 255, 255, 0.1)",
                border: "1px solid rgba(255, 255, 255, 0.2)",
                color: "#e4e4e7",
                padding: "0.5rem 1rem",
                borderRadius: "8px",
                cursor: "pointer",
                fontSize: "0.875rem",
              }}
            >
              ✕ Close
            </button>
          </div>

          {selectedStage.notes && (
            <div style={{
              background: "rgba(59, 130, 246, 0.1)",
              border: "1px solid rgba(59, 130, 246, 0.3)",
              borderRadius: "12px",
              padding: "1rem",
              marginBottom: "1.5rem",
            }}>
              <div style={{ fontSize: "0.875rem", fontWeight: "600", color: "#93c5fd", marginBottom: "0.5rem" }}>
                💡 Notes
              </div>
              <div style={{ color: "#d1d5db" }}>{selectedStage.notes}</div>
            </div>
          )}

          {selectedStage.execution_time_ms && (
            <div style={{ marginBottom: "1.5rem" }}>
              <span style={{ color: "#9ca3af" }}>⚡ Execution Time: </span>
              <span style={{ color: "#10b981", fontWeight: "600" }}>
                {selectedStage.execution_time_ms} ms
              </span>
            </div>
          )}

          {selectedStage.code_snippet && (
            <details style={{ marginTop: "1rem" }}>
              <summary style={{
                cursor: "pointer",
                padding: "1rem",
                background: "rgba(255, 255, 255, 0.05)",
                borderRadius: "12px",
                fontWeight: "600",
                color: "#f59e0b",
                userSelect: "none",
              }}>
                👨‍💻 View Code Implementation
              </summary>
              <pre style={{
                background: "#0a0a0a",
                padding: "1.5rem",
                borderRadius: "12px",
                overflowX: "auto",
                fontSize: "0.85rem",
                marginTop: "1rem",
                border: "1px solid rgba(255, 255, 255, 0.1)",
                color: "#e4e4e7",
                lineHeight: "1.6",
              }}>
                {selectedStage.code_snippet}
              </pre>
            </details>
          )}
        </div>
      )}
    </div>
  );
}

export default App;