import React, { useEffect, useState, useRef } from "react";

function App() {
  const [pipelines, setPipelines] = useState([]);
  const [selectedPipeline, setSelectedPipeline] = useState(null);
  const [selectedStage, setSelectedStage] = useState(null);
  const [showHome, setShowHome] = useState(true);
  const [selectedComplexity, setSelectedComplexity] = useState("all");
  const [selectedTags, setSelectedTags] = useState([]);

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

  // Helper function to get pipeline complexity
  const getPipelineComplexity = (pipeline) => {
    const hasBranching = pipeline.stages.some(s => s.stage_type === 'data_branching');
    const stageCount = pipeline.stages.length;
    
    if (hasBranching || stageCount >= 5) return "advanced";
    if (stageCount >= 3) return "intermediate";
    return "beginner";
  };

  // Helper function to get pipeline tags
  const getPipelineTags = (pipeline) => {
    const tags = [];
    if (pipeline.source_type === "api") tags.push("API");
    if (pipeline.source_type === "file") tags.push("CSV");
    if (pipeline.stages.some(s => s.stage_type === 'data_branching')) tags.push("Branching");
    if (pipeline.stages.some(s => s.stage_type === 'data_transformation')) tags.push("Transform");
    if (pipeline.stages.some(s => s.stage_type === 'data_loading')) tags.push("Database");
    return tags;
  };

  // Toggle tag selection
  const toggleTag = (tag) => {
    setSelectedTags(prev => 
      prev.includes(tag) 
        ? prev.filter(t => t !== tag)
        : [...prev, tag]
    );
  };

  // Filter pipelines based on complexity and tags
  const filteredPipelines = pipelines.filter(pipeline => {
    // Check complexity filter
    if (selectedComplexity !== "all" && getPipelineComplexity(pipeline) !== selectedComplexity) {
      return false;
    }
    
    // Check tag filters (pipeline must have ALL selected tags)
    if (selectedTags.length > 0) {
      const pipelineTags = getPipelineTags(pipeline);
      const hasAllTags = selectedTags.every(tag => pipelineTags.includes(tag));
      if (!hasAllTags) return false;
    }
    
    return true;
  });

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
            lineHeight: "1.4",
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

          {/* Complexity Dropdown */}
          <select
            value={selectedComplexity}
            onChange={(e) => setSelectedComplexity(e.target.value)}
            style={{
              width: "100%",
              background: "rgba(255, 255, 255, 0.05)",
              border: "1px solid rgba(255, 255, 255, 0.1)",
              color: "#e4e4e7",
              padding: "0.75rem",
              borderRadius: "8px",
              fontSize: "0.875rem",
              fontWeight: "500",
              cursor: "pointer",
              marginBottom: "1rem",
              transition: "all 0.3s ease",
            }}
            onMouseEnter={(e) => {
              e.target.style.background = "rgba(255, 255, 255, 0.08)";
              e.target.style.borderColor = "rgba(102, 126, 234, 0.3)";
            }}
            onMouseLeave={(e) => {
              e.target.style.background = "rgba(255, 255, 255, 0.05)";
              e.target.style.borderColor = "rgba(255, 255, 255, 0.1)";
            }}
          >
            <option value="all" style={{ background: "#1a1a2e", color: "#e4e4e7" }}>All Complexity Levels</option>
            <option value="beginner" style={{ background: "#1a1a2e", color: "#e4e4e7" }}>Beginner</option>
            <option value="intermediate" style={{ background: "#1a1a2e", color: "#e4e4e7" }}>Intermediate</option>
            <option value="advanced" style={{ background: "#1a1a2e", color: "#e4e4e7" }}>Advanced</option>
          </select>

          {/* Filter Tags */}
          <div style={{ marginBottom: "1.5rem" }}>
            <div style={{
              fontSize: "0.75rem",
              textTransform: "uppercase",
              letterSpacing: "0.1em",
              color: "#9ca3af",
              marginBottom: "0.5rem",
            }}>
              Filter by Tags
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
              {["API", "CSV", "Branching", "Transform", "Database"].map((tag) => (
                <button
                  key={tag}
                  onClick={() => toggleTag(tag)}
                  style={{
                    background: selectedTags.includes(tag) 
                      ? "rgba(102, 126, 234, 0.3)" 
                      : "rgba(102, 126, 234, 0.1)",
                    border: selectedTags.includes(tag)
                      ? "1px solid rgba(102, 126, 234, 0.6)"
                      : "1px solid rgba(102, 126, 234, 0.3)",
                    color: selectedTags.includes(tag) ? "#c7d2fe" : "#a5b4fc",
                    padding: "0.375rem 0.75rem",
                    borderRadius: "6px",
                    fontSize: "0.75rem",
                    fontWeight: "600",
                    cursor: "pointer",
                    transition: "all 0.3s ease",
                  }}
                  onMouseEnter={(e) => {
                    e.target.style.background = "rgba(102, 126, 234, 0.2)";
                    e.target.style.transform = "translateY(-2px)";
                  }}
                  onMouseLeave={(e) => {
                    e.target.style.background = selectedTags.includes(tag) 
                      ? "rgba(102, 126, 234, 0.3)" 
                      : "rgba(102, 126, 234, 0.1)";
                    e.target.style.transform = "translateY(0)";
                  }}
                >
                  {tag}
                </button>
              ))}
            </div>
            {/* Clear filters button */}
            <div style={{ display: 'flex', marginTop: '0.75rem' }}>
              <button
                onClick={() => { setSelectedComplexity('all'); setSelectedTags([]); }}
                style={{
                  width: '100%',
                  background: 'transparent',
                  border: '2px solid rgba(255,255,255,0.18)',
                  color: '#f3f4f6',
                  padding: '8px 12px',
                  borderRadius: '10px',
                  fontSize: '0.9rem',
                  fontWeight: 700,
                  cursor: 'pointer',
                  textAlign: 'center',
                }}
                title="Clear complexity and tag filters"
              >
                Clear filters
              </button>
            </div>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
            {filteredPipelines.map((p) => {
              const complexity = getPipelineComplexity(p);
              const complexityColors = {
                beginner: "#10B981",
                intermediate: "#F59E0B",
                advanced: "#EF4444"
              };
              
              return (
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
                  <div style={{ 
                    display: "flex", 
                    alignItems: "flex-start", 
                    justifyContent: "space-between",
                    marginBottom: "0.25rem",
                    gap: "1rem"
                  }}>
                    <span style={{ flex: 1, wordWrap: "break-word", marginRight: "0.5rem" }}>{p.pipeline_name}</span>
                    <span style={{
                      background: complexityColors[complexity] + "33",
                      color: complexityColors[complexity],
                      padding: "0.125rem 0.5rem",
                      borderRadius: "4px",
                      fontSize: "0.625rem",
                      fontWeight: "700",
                      textTransform: "uppercase",
                      letterSpacing: "0.05em",
                      border: `1px solid ${complexityColors[complexity]}66`,
                      flexShrink: 0,
                      whiteSpace: "nowrap",
                      marginTop: "0.125rem"
                    }}>
                      {complexity}
                    </span>
                  </div>
                  <div style={{ fontSize: "0.75rem", color: "#9ca3af" }}>
                    {p.stages.length} stages • {p.source_type}
                  </div>
                </button>
              );
            })}
            {filteredPipelines.length === 0 && (
              <div style={{ 
                textAlign: "center", 
                color: "#9ca3af", 
                fontSize: "0.875rem",
                marginTop: "2rem",
                padding: "1rem"
              }}>
                No pipelines match your filters
              </div>
            )}
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

// Modal Overlay Component
function ModalOverlay({ isOpen, onClose, children }) {
  if (!isOpen) return null;

  return (
    <div
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: "rgba(0, 0, 0, 0.7)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: "linear-gradient(135deg, rgba(26, 26, 46, 0.95), rgba(22, 33, 62, 0.95))",
          borderRadius: "24px",
          maxWidth: "700px",
          maxHeight: "90vh",
          overflowY: "auto",
          border: "1px solid rgba(102, 126, 234, 0.3)",
          boxShadow: "0 20px 60px rgba(0, 0, 0, 0.5)",
          backdropFilter: "blur(10px)",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </div>
    </div>
  );
}

// Home View Component
function HomeView() {
  const [selectedCard, setSelectedCard] = useState(null);

  const conceptCards = [
    {
      id: "visualization",
      icon: "📊",
      title: "Why Visualize Workflows?",
      description: "Data pipelines are complex systems that transform raw data through multiple stages. Visualization helps engineers and stakeholders understand data flow, identify bottlenecks, and optimize performance. A clear visual representation of your workflow is essential for maintaining data integrity and system reliability.",
      detail: "Workflows drive business value. From ETL processes to machine learning pipelines, understanding how data moves through your systems is critical for decision-making and debugging.",
      content: "When you can see your data moving through a pipeline, you gain immediate clarity on where bottlenecks occur and how different systems interact. Visual representations make it easier to:\n\n• Identify performance issues quickly\n• Communicate complex flows to stakeholders\n• Debug data quality problems\n• Plan for scalability\n\nMany organizations report that implementing proper pipeline visualization reduced troubleshooting time by 40-60%."
    },
    {
      id: "understanding",
      icon: "🔍",
      title: "Understanding Data Stages",
      description: "Each stage in a data pipeline performs specific transformations. From data ingestion to final loading, understanding what happens at each stage helps you trace data quality issues and optimize performance.",
      detail: "Every transformation matters. Whether it's cleaning, validating, or aggregating—each stage plays a crucial role in the overall pipeline success.",
      content: "A typical data pipeline consists of several key stages:\n\n• Data Ingestion: Extracting data from source systems\n• Data Cleaning: Removing errors and inconsistencies\n• Data Transformation: Converting data into the desired format\n• Data Validation: Ensuring quality standards\n• Data Loading: Moving processed data to destinations\n\nEach stage is critical. A single error in any stage can cascade through your entire pipeline."
    },
    {
      id: "architecture",
      icon: "⚙️",
      title: "Pipeline Architecture Patterns",
      description: "Learn industry-standard patterns for building scalable and maintainable data pipelines. From linear flows to complex branching logic, discover how successful organizations structure their data operations.",
      detail: "Good architecture enables growth. Understanding design patterns helps you build systems that scale with your data needs.",
      content: "Modern data architectures follow proven patterns:\n\n• Linear Pipelines: Simple, sequential data flows\n• Branching Pipelines: Different processing paths based on data characteristics\n• Fan-out/Fan-in: Parallel processing for performance\n• Event-driven: Real-time processing triggered by events\n\nChoosing the right pattern depends on your data volume, latency requirements, and business needs."
    },
    {
      id: "career",
      icon: "🚀",
      title: "Launch Your Data Career",
      description: "Data engineering is one of the fastest-growing fields in tech. DataJourney prepares you for real-world challenges by teaching through hands-on exploration of production-like pipelines.",
      detail: "The future is data-driven. Master pipeline concepts now and position yourself for high-demand data engineering roles.",
      content: "Data engineering roles are in high demand with competitive salaries:\n\n• Average salary: $120,000 - $180,000+\n• Job growth: 15-20% annually\n• Top skills: Python, SQL, Apache Spark, Kubernetes\n• Industries: Tech, Finance, Healthcare, E-commerce\n\nBy mastering pipeline concepts now, you're positioning yourself for one of tech's most rewarding careers."
    }
  ];

  return (
    <div style={{ maxWidth: "1000px", margin: "0 auto" }}>
      <div style={{
        background: "rgba(102, 126, 234, 0.1)",
        border: "1px solid rgba(102, 126, 234, 0.3)",
        borderRadius: "20px",
        padding: "3rem",
        marginBottom: "3rem",
      }}>
        <h1 style={{
          fontSize: "3rem",
          lineHeight: "1.1",
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
        {conceptCards.map((card) => (
          <InfoCard
            key={card.id}
            card={card}
            onClick={() => setSelectedCard(card)}
          />
        ))}
      </div>

      {/* Modal for detailed view */}
      <ModalOverlay isOpen={!!selectedCard} onClose={() => setSelectedCard(null)}>
        {selectedCard && (
          <div style={{ padding: "2.5rem" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "start", marginBottom: "2rem" }}>
              <div>
                <h2 style={{
                  fontSize: "2rem",
                  marginTop: 0,
                  marginBottom: "0.5rem",
                  background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
                  WebkitBackgroundClip: "text",
                  WebkitTextFillColor: "transparent",
                }}>
                  {selectedCard.icon} {selectedCard.title}
                </h2>
                <p style={{ color: "#d1d5db", fontSize: "0.95rem", margin: 0 }}>
                  {selectedCard.detail}
                </p>
              </div>
              <button
                onClick={() => setSelectedCard(null)}
                style={{
                  background: "rgba(255, 255, 255, 0.1)",
                  border: "1px solid rgba(255, 255, 255, 0.2)",
                  color: "#e4e4e7",
                  padding: "0.5rem 1rem",
                  borderRadius: "8px",
                  cursor: "pointer",
                  fontSize: "1.2rem",
                  fontWeight: "bold",
                  width: "40px",
                  height: "40px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  flexShrink: 0,
                  transition: "all 0.2s ease",
                }}
                onMouseEnter={(e) => {
                  e.target.style.background = "rgba(255, 255, 255, 0.2)";
                }}
                onMouseLeave={(e) => {
                  e.target.style.background = "rgba(255, 255, 255, 0.1)";
                }}
              >
                ✕
              </button>
            </div>

            {/* Image Placeholder */}
            <div style={{
              background: "rgba(102, 126, 234, 0.1)",
              border: "2px dashed rgba(102, 126, 234, 0.3)",
              borderRadius: "12px",
              height: "250px",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              marginBottom: "2rem",
            }}>
              <div style={{ textAlign: "center" }}>
                <div style={{ fontSize: "4rem", marginBottom: "0.5rem" }}>🖼️</div>
                <p style={{ color: "#9ca3af", margin: 0 }}>Image placeholder</p>
              </div>
            </div>

            {/* Description */}
            <p style={{ color: "#d1d5db", lineHeight: "1.8", marginBottom: "1.5rem" }}>
              {selectedCard.description}
            </p>

            {/* Main content */}
            <div style={{
              background: "rgba(255, 255, 255, 0.05)",
              border: "1px solid rgba(255, 255, 255, 0.1)",
              borderRadius: "12px",
              padding: "1.5rem",
              color: "#d1d5db",
              lineHeight: "1.8",
              whiteSpace: "pre-wrap",
              fontSize: "0.95rem",
            }}>
              {selectedCard.content}
            </div>
          </div>
        )}
      </ModalOverlay>

      <div style={{
        background: "rgba(0, 0, 0, 0.3)",
        borderRadius: "16px",
        padding: "2rem",
        border: "1px solid rgba(255, 255, 255, 0.1)",
      }}>
        <h2 style={{ marginTop: 0, color: "#f59e0b" }}>The Data Engineering Landscape</h2>
        
        <div style={{ marginBottom: "1.5rem" }}>
          <h3 style={{ color: "#a5b4fc", fontSize: "1.1rem", marginTop: "1rem", marginBottom: "0.5rem" }}>Market Growth & Demand</h3>
          <p style={{ lineHeight: "1.8", color: "#d1d5db", margin: "0.5rem 0" }}>
            The global data engineering market was valued at <strong>$8.2 billion in 2023</strong> and is projected to reach <strong>$25.6 billion by 2030</strong>, representing a compound annual growth rate (CAGR) of 16.8%. Data engineering positions have grown <strong>74% faster</strong> than software engineering roles over the past five years.
          </p>
        </div>

        <div style={{ marginBottom: "1.5rem" }}>
          <h3 style={{ color: "#a5b4fc", fontSize: "1.1rem", marginTop: "1rem", marginBottom: "0.5rem" }}>Compensation & Employment</h3>
          <p style={{ lineHeight: "1.8", color: "#d1d5db", margin: "0.5rem 0" }}>
            Senior data engineers in the U.S. earn an average of <strong>$185,000-$210,000 annually</strong>, including bonuses and stock options. Entry-level positions start at <strong>$95,000-$125,000</strong>. The field employs over <strong>250,000 professionals globally</strong>, with demand outpacing supply by 3:1 in major tech hubs.
          </p>
        </div>

        <div style={{ marginBottom: "1.5rem" }}>
          <h3 style={{ color: "#a5b4fc", fontSize: "1.1rem", marginTop: "1rem", marginBottom: "0.5rem" }}>Industry Challenges</h3>
          <p style={{ lineHeight: "1.8", color: "#d1d5db", margin: "0.5rem 0" }}>
            <strong>80% of data science projects fail</strong> due to poor data engineering and pipeline infrastructure. Organizations cite data quality, pipeline reliability, and scalability as top challenges. <strong>45% of data engineers spend over 50% of their time on data cleaning and validation</strong> tasks, highlighting the critical need for well-designed pipelines.
          </p>
        </div>

        <div style={{ marginBottom: "1.5rem" }}>
          <h3 style={{ color: "#a5b4fc", fontSize: "1.1rem", marginTop: "1rem", marginBottom: "0.5rem" }}>Technology Trends</h3>
          <p style={{ lineHeight: "1.8", color: "#d1d5db", margin: "0.5rem 0" }}>
            Cloud data platforms (AWS, Google Cloud, Azure) now host <strong>92% of new enterprise data pipelines</strong>. Apache Spark remains the most widely used framework for distributed data processing, with <strong>89% adoption among Fortune 500 companies</strong>. Real-time streaming architectures are growing <strong>3x faster</strong> than batch processing systems.
          </p>
        </div>

        <div style={{ 
          background: "rgba(102, 126, 234, 0.1)",
          border: "1px solid rgba(102, 126, 234, 0.3)",
          borderRadius: "12px",
          padding: "1.5rem",
          marginTop: "1.5rem"
        }}>
          <p style={{ margin: "0 0 1rem 0", color: "#c7d2fe", fontWeight: "600" }}>📚 Data Sources & References:</p>
          <div style={{ fontSize: "0.85rem", color: "#9ca3af", lineHeight: "1.6" }}>
            <p style={{ margin: "0.5rem 0" }}>• Grand View Research (2023) - Data Engineering Market Size, Share & Trends Analysis</p>
            <p style={{ margin: "0.5rem 0" }}>• LinkedIn Jobs Report (2024) - Data Engineering hiring trends and salary benchmarks</p>
            <p style={{ margin: "0.5rem 0" }}>• Gartner (2023) - Magic Quadrant for Cloud Data Platforms</p>
            <p style={{ margin: "0.5rem 0" }}>• McKinsey & Company (2023) - Why 80% of Data Science Projects Fail</p>
            <p style={{ margin: "0.5rem 0" }}>• Stack Overflow Developer Survey (2024) - Data Engineering Tools & Technologies</p>
            <p style={{ margin: "0.5rem 0" }}>• Blind Compensation Database (2024) - Tech Sector Salary Data</p>
          </div>
        </div>
      </div>
    </div>
  );
}

// Info Card Component
function InfoCard({ card, onClick }) {
  return (
    <div
      onClick={onClick}
      style={{
        background: "rgba(255, 255, 255, 0.05)",
        border: "1px solid rgba(255, 255, 255, 0.1)",
        borderRadius: "16px",
        padding: "2rem",
        transition: "all 0.3s ease",
        cursor: "pointer",
        position: "relative",
        overflow: "hidden",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = "rgba(255, 255, 255, 0.08)";
        e.currentTarget.style.transform = "translateY(-8px)";
        e.currentTarget.style.borderColor = "rgba(102, 126, 234, 0.5)";
        e.currentTarget.style.boxShadow = "0 15px 40px rgba(102, 126, 234, 0.25)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = "rgba(255, 255, 255, 0.05)";
        e.currentTarget.style.transform = "translateY(0)";
        e.currentTarget.style.borderColor = "rgba(255, 255, 255, 0.1)";
        e.currentTarget.style.boxShadow = "none";
      }}
    >
      <div style={{ fontSize: "3rem", marginBottom: "1rem" }}>{card.icon}</div>
      <h3 style={{ margin: "0.5rem 0 1rem 0", color: "#e4e4e7", fontSize: "1.15rem" }}>{card.title}</h3>
      <p style={{ margin: 0, color: "#9ca3af", lineHeight: "1.6", fontSize: "0.95rem" }}>
        {card.description}
      </p>
      <div style={{
        marginTop: "1rem",
        paddingTop: "1rem",
        borderTop: "1px solid rgba(102, 126, 234, 0.2)",
        color: "#667eea",
        fontSize: "0.85rem",
        fontWeight: "600",
      }}>
        Click to learn more →
      </div>
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
      }, 100);
    };

    updatePositions();

    observer = new ResizeObserver(() => {
      requestAnimationFrame(updatePositions);
    });
    
    if (containerRef.current) {
      observer.observe(containerRef.current);
    }

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

// // Branching DAG (for pipelines with branches)
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