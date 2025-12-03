# ProtoChangeMiner

ProtoChangeMiner is a research-oriented mining tool that analyzes **co-change relationships between Protobuf (.proto) files and other source files** in **gRPC-based microservices**.
This tool extracts commit-level transactions from large-scale microservice repositories and identifies **which files tend to be modified together with proto files**, using metrics such as **support**, **confidence**, and **lift**.

---

## ✨ Features

- 🔍 **Commit-level transaction mining**
  Extracts file-level changes from Git history for microservices.

- 📦 **Focus on .proto files**
  Only commits including Protobuf file changes are analyzed.

- 📊 **Co-change metric calculation**
  Computes support, confidence, and lift for each pair `(proto, file)`.

- 📈 **Repository-level ranking**
  Generates CSV output showing files that frequently change together with proto files.

---