​🛡️ Enterprise Secure AI Pipeline: Diabetes Prediction
​End-to-End MLOps Framework with Data Security & Quality Governance
​📖 Overview
​This project implements a production-grade Machine Learning pipeline designed for healthcare data. It integrates Data Engineering, Cybersecurity (ISC² Standards), and MLOps into a single, cohesive system. The pipeline is built to be environment-agnostic, running seamlessly on Microsoft Fabric, Databricks, or Google Colab.
​✨ Key Features
​Security-First Design: Implements SHA-256 hashing for PII (Personally Identifiable Information) and input sanitization to prevent injection attacks.
​Data Quality Guardrails: Automated audit layer to detect duplicates and enforce schema integrity.
​Scalable Architecture: Built using Object-Oriented Programming (OOP) for high modularity.
​Full Experiment Tracking: Managed via MLflow for model versioning, parameter logging, and deployment readiness.
​🛠️ Tech Stack
​Language: Python 3.x
​Big Data: Apache Spark (PySpark)
​Machine Learning: Scikit-Learn (DecisionTreeRegressor)
​Lifecycle Management: MLflow
​Security: SHA-256 Hashing & Regex Sanitization
​🚀 Pipeline Architecture
​Ingestion & Security: Load Delta tables, anonymize sensitive IDs, and sanitize strings.
​Quality Audit: Remove duplicates and handle missing values (Nulls).
​Training: Feature engineering, model fitting, and automated logging.
​Registration: Governance through the MLflow Model Registry.
​💻 How to Run
