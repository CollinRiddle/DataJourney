import React, { useEffect, useState, useRef } from "react";

function App() {
  const [pipelines, setPipelines] = useState([]);
  const [selectedPipeline, setSelectedPipeline] = useState(null);
  const [selectedStage, setSelectedStage] = useState(null);
  const [showHome, setShowHome] = useState(true);

  // Fetch pipelines from the backend
  useEffect(() => {
    console.log("🔍 Fetching pipelines from /api/pipelines...");
    fetch("/api/pipelines")
      .then((res) => {
        console.log("📡 Response status:", res.status);
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
        }
        return res.json();
      })
      .then((data) => {
        console.log("✅ Pipelines loaded:", data);
        setPipelines(data);
      })
      .catch((err) => {
        console.error("❌ Error fetching pipelines:", err);
        console.error("Make sure Flask backend is running on http://127.0.0.1:5000");
      });
  }, []);

  // Stage colors based on type
  const stageColors = {
    data_ingestion: "#10B981",
    data_transformation: "#F59E0B",
    data_loading: "#8B5CF6",
    data_cleaning: "#3B82F6",
    data_branching: "#EC4899",
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
          {/* Logo placeholder - uncomment when ready */}
          {/* <img src="/logo.svg" alt="DataJourney" style={{ height: "40px" }} /> */}
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
                  {p.stages.length} stages • {p.source_type}
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
          Welcome to DataJourney 🚀
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

function PipelineView({ pipeline, selectedStage, onStageClick, stageColors }) {
  const hasBranching = pipeline.stages.some(s => s.stage_type === 'data_branching');
  const [zoom, setZoom] = useState(1);

  const zoomButtonStyle = {
    background: "rgba(255, 255, 255, 0.1)",
    border: "1px solid rgba(255, 255, 255, 0.2)",
    color: "#fff",
    fontSize: "1rem",
    borderRadius: "6px",
    width: "32px",
    height: "32px",
    cursor: "pointer",
    transition: "all 0.2s ease",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    padding: 0,
  };

  return (
    <div>
      {/* Pipeline Header */}
      <div style={{ marginBottom: "1rem" }}>
        <h2
          style={{
            fontSize: "2rem",
            marginTop: 0,
            marginBottom: "0.5rem",
            color: "#f3f4f6",
          }}
        >
          {pipeline.pipeline_name}
        </h2>
        <p style={{ color: "#9ca3af", marginBottom: "1rem" }}>
          {pipeline.description}
        </p>
        <div
          style={{
            display: "flex",
            gap: "2rem",
            fontSize: "0.9rem",
            flexWrap: "wrap",
          }}
        >
          <div>
            <span style={{ color: "#9ca3af" }}>Last Run: </span>
            <span style={{ color: "#d1d5db" }}>
              {pipeline.last_run
                ? new Date(pipeline.last_run).toLocaleString()
                : "Never"}
            </span>
          </div>
          <div>
            <span style={{ color: "#9ca3af" }}>Output: </span>
            <span style={{ color: "#d1d5db" }}>
              {pipeline.final_output?.database_table || "Not specified"}
            </span>
          </div>
          <div>
            <span style={{ color: "#9ca3af" }}>Source: </span>
            <span
              style={{
                color: "#d1d5db",
                textTransform: "uppercase",
              }}
            >
              {pipeline.source_type}
            </span>
          </div>
        </div>
      </div>

      {/* Zoom Controls */}
      <div style={{
        display: "flex",
        justifyContent: "flex-end",
        marginBottom: "1rem",
      }}>
        <div
          style={{
            display: "flex",
            gap: "0.5rem",
            background: "rgba(0, 0, 0, 0.5)",
            padding: "4px",
            borderRadius: "8px",
            backdropFilter: "blur(4px)",
          }}
        >
          <button
            onClick={() => setZoom((z) => Math.min(z + 0.1, 2))}
            style={zoomButtonStyle}
            title="Zoom In"
          >
            ➕
          </button>
          <button
            onClick={() => setZoom((z) => Math.max(z - 0.1, 0.5))}
            style={zoomButtonStyle}
            title="Zoom Out"
          >
            ➖
          </button>
          <button
            onClick={() => setZoom(1)}
            style={zoomButtonStyle}
            title="Reset Zoom"
          >
            🔄
          </button>
        </div>
      </div>

      {/* DAG Visualization with Scroll & Zoom */}
      <div
        style={{
          background: "rgba(0, 0, 0, 0.3)",
          borderRadius: "20px",
          padding: "1rem",
          marginBottom: "2rem",
          border: "1px solid rgba(255, 255, 255, 0.1)",
          overflow: "auto",
          maxHeight: "70vh",
          position: "relative",
          scrollPaddingTop: "50px"
        }}
      >


        {/* Scrollable and Zoomable DAG Container */}
        <div
          style={{
            transform: `scale(${zoom})`,
            transformOrigin: "center top",
            transition: "transform 0.25s ease",
            display: "inline-block",
            minWidth: "100%",
          }}
        >
          <h3
            style={{
              fontSize: "0.875rem",
              textTransform: "uppercase",
              letterSpacing: "0.1em",
              color: "#9ca3af",
              marginTop: 0,
              marginBottom: "2rem",
              textAlign: "center",
            }}
          >
            Pipeline Flow {hasBranching && "• Branching Logic"}
          </h3>

          {hasBranching ? (
            <BranchingDAG
              stages={pipeline.stages}
              selectedStage={selectedStage}
              onStageClick={onStageClick}
              stageColors={stageColors}
            />
          ) : (
            <LinearDAG
              stages={pipeline.stages}
              selectedStage={selectedStage}
              onStageClick={onStageClick}
              stageColors={stageColors}
            />
          )}
        </div>
      </div>

      {/* Stage Details Panel */}
      {selectedStage && (
        <StageDetailsPanel
          stage={selectedStage}
          stageColor={stageColors[selectedStage.stage_type]}
          onClose={() => onStageClick(selectedStage)}
        />
      )}
    </div>
  );
}

// Dynamic Arrow Component
function DynamicArrow({ start, end, color = "#4b5563" }) {
  if (!start || !end || !start.x || !end.x) return null;

  // Create a unique ID for each arrow marker
  const markerId = `arrowhead-${color.replace('#', '')}-${Math.random().toString(36).substr(2, 9)}`;
  
  return (
    <svg
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: '100%',
        height: '100%',
        pointerEvents: 'none',
        overflow: 'visible',
      }}
    >
      <defs>
        <marker
          id={markerId}
          markerWidth="10"
          markerHeight="7"
          refX="9"
          refY="3.5"
          orient="auto"
        >
          <polygon points="0 0, 10 3.5, 0 7" fill={color} />
        </marker>
      </defs>
      <line
        x1={start.x + start.width}
        y1={start.y + start.height / 2}
        x2={end.x}
        y2={end.y + end.height / 2}
        stroke={color}
        strokeWidth="2"
        markerEnd={`url(#${markerId})`}
      />
    </svg>
  );
}

// Linear DAG (for non-branching pipelines)
function LinearDAG({ stages, selectedStage, onStageClick, stageColors }) {
  const [nodePositions, setNodePositions] = useState({});
  const containerRef = useRef(null);

  // Update node positions when stages change or on resize
  useEffect(() => {
    let observer;
    const updatePositions = () => {
      if (!containerRef.current) return;
      
      setTimeout(() => {
        const positions = {};
        stages.forEach(stage => {
          const element = document.getElementById(`stage-${stage.stage_id}`);
          if (element && containerRef.current) {
            const rect = element.getBoundingClientRect();
            const containerRect = containerRef.current.getBoundingClientRect();
            positions[stage.stage_id] = {
              x: rect.x - containerRect.x,
              y: rect.y - containerRect.y,
              width: rect.width,
              height: rect.height
            };
          }
        });
        setNodePositions(positions);
      }, 100); // Small delay to ensure DOM is ready
    };

    // Initial position update
    updatePositions();

    // Update positions on resize
    observer = new ResizeObserver(() => {
      requestAnimationFrame(updatePositions);
    });
    
    if (containerRef.current) {
      observer.observe(containerRef.current);
    }

    // Cleanup
    return () => {
      if (observer) {
        observer.disconnect();
      }
    };
  }, [stages]);

  return (
    <div
      ref={containerRef}
      style={{
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        gap: "4rem",
        flexWrap: "wrap",
        position: "relative",
        padding: "2rem",
      }}
    >
      {stages.map((stage, index) => (
        <div 
          key={stage.stage_id} 
          id={`stage-${stage.stage_id}`}
          style={{ position: "relative" }}
        >
          <StageNode
            stage={stage}
            isSelected={selectedStage?.stage_id === stage.stage_id}
            onClick={() => onStageClick(stage)}
            color={stageColors[stage.stage_type]}
          />
        </div>
      ))}
      
      {/* Draw arrows between stages */}
      {stages.map((stage, index) => {
        if (index < stages.length - 1) {
          const startPos = nodePositions[stage.stage_id];
          const endPos = nodePositions[stages[index + 1].stage_id];
          return (
            <DynamicArrow
              key={`arrow-${stage.stage_id}`}
              start={startPos}
              end={endPos}
              color={stageColors[stage.stage_type]}
            />
          );
        }
        return null;
      })}
    </div>
  );
}

// Branching DAG (for pipelines with branches)
function BranchingDAG({ stages, selectedStage, onStageClick, stageColors }) {
  const [nodePositions, setNodePositions] = useState({});
  const containerRef = useRef(null);

  // Find branch point and merge point
  const branchIndex = stages.findIndex(s => s.stage_type === 'data_branching');
  const beforeBranch = stages.slice(0, branchIndex + 1);
  const branchStages = stages.slice(branchIndex + 1, -1);
  const mergeStage = stages[stages.length - 1];

  // Update node positions when stages change or on resize
  useEffect(() => {
    let observer;
    const updatePositions = () => {
      if (!containerRef.current) return;
      
      setTimeout(() => {
        const positions = {};
        stages.forEach(stage => {
          const element = document.getElementById(`stage-${stage.stage_id}`);
          if (element && containerRef.current) {
            const rect = element.getBoundingClientRect();
            const containerRect = containerRef.current.getBoundingClientRect();
            positions[stage.stage_id] = {
              x: rect.x - containerRect.x,
              y: rect.y - containerRect.y,
              width: rect.width,
              height: rect.height
            };
          }
        });
        setNodePositions(positions);
      }, 100); // Small delay to ensure DOM is ready
    };

    // Initial position update
    updatePositions();

    // Update positions on resize
    observer = new ResizeObserver(() => {
      requestAnimationFrame(updatePositions);
    });
    
    if (containerRef.current) {
      observer.observe(containerRef.current);
    }

    window.addEventListener('load', updatePositions);

    // Cleanup
    return () => {
      if (observer) {
        observer.disconnect();
      }
      window.removeEventListener('load', updatePositions);
    };
  }, [stages]);

  return (
    <div ref={containerRef} style={{ 
      position: "relative",
      padding: "4rem",
      minHeight: "200px",
    }}>
      {/* All stages in a single horizontal line */}
      <div style={{ 
        display: "flex",
        alignItems: "center",
        gap: "4rem",
        justifyContent: "center",
        position: "relative",
      }}>
        {/* Stages before branch */}
        {beforeBranch.map((stage) => (
          <div 
            key={stage.stage_id}
            id={`stage-${stage.stage_id}`}
            style={{ position: "relative" }}
          >
            <StageNode 
              stage={stage}
              isSelected={selectedStage?.stage_id === stage.stage_id}
              onClick={() => onStageClick(stage)}
              color={stageColors[stage.stage_type]}
            />
          </div>
        ))}

        {/* Branch stages - vertical stack */}
        <div style={{ 
          display: "flex",
          flexDirection: "column",
          gap: "2rem",
          position: "relative",
          justifyContent: "center",
          margin: "0 2rem"
        }}>
          {branchStages.map((stage) => (
            <div 
              key={stage.stage_id}
              id={`stage-${stage.stage_id}`}
              style={{ position: "relative" }}
            >
              <StageNode 
                stage={stage}
                isSelected={selectedStage?.stage_id === stage.stage_id}
                onClick={() => onStageClick(stage)}
                color={stageColors[stage.stage_type]}
                branchLabel={stage.branch_path}
              />
            </div>
          ))}
        </div>

        {/* Merge stage */}
        <div 
          id={`stage-${mergeStage.stage_id}`}
          style={{ position: "relative" }}
        >
          <StageNode 
            stage={mergeStage}
            isSelected={selectedStage?.stage_id === mergeStage.stage_id}
            onClick={() => onStageClick(mergeStage)}
            color={stageColors[mergeStage.stage_type]}
          />
        </div>
      </div>

      {/* Draw arrows */}
      {/* Before branch arrows */}
      {beforeBranch.map((stage, index) => {
        if (index < beforeBranch.length - 1) {
          const startPos = nodePositions[stage.stage_id];
          const endPos = nodePositions[beforeBranch[index + 1].stage_id];
          return (
            <DynamicArrow
              key={`arrow-${stage.stage_id}`}
              start={startPos}
              end={endPos}
              color={stageColors[stage.stage_type]}
            />
          );
        }
        return null;
      })}

      {/* Branch arrows */}
      {branchStages.map((stage) => {
        const branchStart = nodePositions[beforeBranch[beforeBranch.length - 1].stage_id];
        const branchPos = nodePositions[stage.stage_id];
        const mergePos = nodePositions[mergeStage.stage_id];
        
        return branchStart && branchPos && mergePos ? (
          <React.Fragment key={`branch-${stage.stage_id}`}>
            {/* Arrow from branch point to branch stage */}
            <DynamicArrow
              start={branchStart}
              end={branchPos}
              color="#EC4899"
            />
            {/* Arrow from branch stage to merge point */}
            <DynamicArrow
              start={branchPos}
              end={mergePos}
              color="#8B5CF6"
            />
          </React.Fragment>
        ) : null;
      })}
    </div>
  );
}

// Stage Node Component
function StageNode({ stage, isSelected, onClick, color, branchLabel }) {
  return (
    <div
      onClick={onClick}
      style={{
        background: isSelected
          ? `linear-gradient(135deg, ${color}dd, ${color})`
          : `${color}33`,
        border: `2px solid ${color}`,
        borderRadius: "16px",
        padding: "1.5rem",
        minWidth: "180px",
        cursor: "pointer",
        transition: "all 0.3s ease",
        position: "relative",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.transform = "scale(1.05) translateY(-4px)";
        e.currentTarget.style.boxShadow = `0 10px 30px ${color}50`;
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
        background: color,
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
      {branchLabel && (
        <div style={{
          marginTop: "0.5rem",
          fontSize: "0.7rem",
          color: "#EC4899",
          fontWeight: "600",
          textTransform: "uppercase",
        }}>
          🔀 {branchLabel}
        </div>
      )}
    </div>
  );
}

// Stage Details Panel Component
function StageDetailsPanel({ stage, stageColor, onClose }) {
  return (
    <div style={{
      background: "rgba(0, 0, 0, 0.4)",
      borderRadius: "20px",
      padding: "2rem",
      border: `2px solid ${stageColor}`,
      animation: "slideIn 0.3s ease",
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "start", marginBottom: "1.5rem" }}>
        <div>
          <h3 style={{
            margin: 0,
            marginBottom: "0.5rem",
            fontSize: "1.5rem",
            color: stageColor,
          }}>
            {stage.stage_name}
          </h3>
          <p style={{ margin: 0, color: "#d1d5db" }}>{stage.description}</p>
        </div>
        <button
          onClick={onClose}
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

      {/* Branch info for branching stages */}
      {stage.branches && (
        <div style={{
          background: "rgba(236, 72, 153, 0.1)",
          border: "1px solid rgba(236, 72, 153, 0.3)",
          borderRadius: "12px",
          padding: "1rem",
          marginBottom: "1.5rem",
        }}>
          <div style={{ fontSize: "0.875rem", fontWeight: "600", color: "#EC4899", marginBottom: "0.5rem" }}>
            🔀 Branch Paths
          </div>
          {stage.branches.map((branch, idx) => (
            <div key={idx} style={{ color: "#d1d5db", marginTop: "0.5rem", fontSize: "0.9rem" }}>
              <strong>{branch.branch_id}:</strong> {branch.condition} → {branch.next_stage}
            </div>
          ))}
        </div>
      )}

      {stage.notes && (
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
          <div style={{ color: "#d1d5db" }}>{stage.notes}</div>
        </div>
      )}

      {stage.execution_time_ms && (
        <div style={{ marginBottom: "1.5rem" }}>
          <span style={{ color: "#9ca3af" }}>⚡ Execution Time: </span>
          <span style={{ color: "#10b981", fontWeight: "600" }}>
            {stage.execution_time_ms} ms
          </span>
        </div>
      )}

      {stage.code_snippet && (
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
            {stage.code_snippet}
          </pre>
        </details>
      )}
    </div>
  );
}

export default App;