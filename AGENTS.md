# SPEC-Driven Development 约束文档
- 参考 Notion Page ID: 37d0ec7a4b9280bd8c84c922f57f6d3e

# 全局研发环境与行为准则
- 参考 Notion Page ID: 37d0ec7a4b928005aee8d863af5b2518

# 当前项目需求文档
- 参考 Notion Page ID: 38a0ec7a4b9280ed9097c48dc7c7f271
- 变更记录 Notion Page ID: 38a0ec7a4b928026934ecb04a5bd6013

# 当前项目设计文档
- 参考 Notion Page ID: 38a0ec7a4b9280d19df8e4308c768077
- 变更记录 Notion Page ID: 38a0ec7a4b92802d922af743a5b1270e

# graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- For cross-module "how does X relate to Y" questions, prefer `graphify query "<question>"`, `graphify path "<A>" "<B>"`, or `graphify explain "<concept>"` over grep — these traverse the graph's EXTRACTED + INFERRED edges instead of scanning files
- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)
