import { memo } from "react";
import { getBezierPath, BaseEdge, type EdgeProps } from "reactflow";

/**
 * Custom edge for Kafka Streams: direct input topic → output topic.
 * Renders animated dashed white line with app name label via BaseEdge.
 */
function StreamsEdgeComponent({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  label,
  markerEnd,
}: EdgeProps) {
  const [path, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  return (
    <BaseEdge
      id={id}
      path={path}
      labelX={labelX}
      labelY={labelY}
      markerEnd={markerEnd}
      style={{
        stroke: "white",
        strokeWidth: 2.5,
        strokeDasharray: "8 4",
        animation: "streams-edge-dash 0.8s linear infinite",
      }}
    />
  );
}

export const StreamsEdge = memo(StreamsEdgeComponent);
