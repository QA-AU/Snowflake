# SCD2 Validation Framework (Snowflake)

## Overview

This repository contains a **read-only, audit-safe SCD Type-2 (SCD2) validation framework** for Snowflake.

The framework validates that an SCD2 load has correctly applied **INSERT, UPDATE, and DELETE logic** by comparing:

- **SOURCE** data (business intent)
- **PRE-SCD2 TARGET** (baseline via Time Travel)
- **EXPECTED POST-SCD2** (logical outcome)
- **ACTUAL POST-SCD2 TARGET**

The validator **does not modify source or target data** and is designed to be:
- deterministic
- explainable
- production-safe
- step-wise and debuggable

---

## Key Design Principles

- ✅ Read-only validation
- ✅ Uses Snowflake **Time Travel** for pre-SCD2 baseline
- ✅ Canonical comparison (handles datatype differences)
- ✅ Step-wise Python execution (each step runnable independently)
- ✅ Minimal persistent outputs (no wide tables)
- ✅ Audit-grade telemetry (10 mandatory fields)
- ✅ One detailed human-readable example per run

---

## High-Level Flow

