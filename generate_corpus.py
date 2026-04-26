import json
from pathlib import Path

OUT = Path.home() / "Node_Temp_Node/training/corpus/generated_core.jsonl"

topics = [
    "quantum_computing",
    "quantum_error_correction",
    "quantum_entanglement",
    "quantum_superposition",
    "distributed_systems",
    "consensus_protocols",
    "dag_consensus",
    "fault_tolerance",
    "network_latency",
    "heterogeneous_compute",
    "gpu_architecture",
    "llm_inference",
    "energy_efficiency",
    "secure_networking",
    "vpn_architecture",
    "packet_inspection",
    "encryption_models",
    "ai_alignment",
    "agent_systems",
    "autonomous_nodes"
]

templates = [
    "Core concept of {topic} in modern systems.",
    "Key operational behavior of {topic}.",
    "Primary performance constraint in {topic}.",
    "Architectural role of {topic} in distributed environments.",
    "Failure modes and recovery patterns in {topic}.",
    "Optimization strategies for {topic}.",
    "Real-world deployment considerations of {topic}."
]

def generate():
    with open(OUT, "w") as f:
        for topic in topics:
            for template in templates:
                entry = {
                    "topic": topic,
                    "type": "concept",
                    "content": template.format(topic=topic),
                    "state": "UNVERIFIED"
                }
                f.write(json.dumps(entry) + "\n")

if __name__ == "__main__":
    generate()
