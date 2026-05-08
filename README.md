# Dataset Engine README

## Overview

This project is a lightweight embedded dataset engine written in pure
Kotlin.

It behaves similarly to a very small database engine by: - Storing
serialized objects in binary files - Maintaining a separate index file -
Supporting insert, update, delete, and read operations - Using
append-only writes for consistency - Supporting file compaction -
Providing asynchronous iteration using Kotlin Flow

Example usage:

``` kotlin
val dataset = Dataset.create<User>("dataset")
dataset.insert(User("Sean", 23))
```

------------------------------------------------------------------------

## Architecture

The system is divided into two abstraction layers:

### Layer 1 --- Storage Layer (Physical File Layer)

Works directly with files and bytes: - Reads/writes binary data -
Manages offsets - Handles `.data` and `.idx` files - Performs low-level
IO via Okio

Files: - `.data` → actual records - `.idx` → metadata index

------------------------------------------------------------------------

### Layer 2 --- Dataset Layer (Logical Data Layer)

Works with: - Kotlin objects - Serialization - Record lifecycle
(insert/update/delete) - In-memory index cache

------------------------------------------------------------------------

## Data Storage Format

### Data File (.data)

Stored as:

JSON → UTF-8 → Base64 → binary write

------------------------------------------------------------------------

### Index File (.idx)

Fixed-size records:

13 bytes total: - 1 byte status - 4 bytes size - 8 bytes start position

------------------------------------------------------------------------

## Record States

-   FINAL → valid record
-   PENDING → writing in progress
-   LOCKED → updating
-   DELETED → logically removed

------------------------------------------------------------------------

## Core Operations

### Insert

1.  Serialize object
2.  Convert to Base64
3.  Append to `.data`
4.  Write `.idx` entry as PENDING → FINAL

------------------------------------------------------------------------

### Get

1.  Lookup in-memory index
2.  Read from `.data`
3.  Decode Base64 → JSON → object

------------------------------------------------------------------------

### Update

-   Old record marked DELETED
-   New record appended (append-only strategy)

------------------------------------------------------------------------

### Delete

-   Mark index as DELETED
-   Zero-fill data region

------------------------------------------------------------------------

## Index System

In-memory map: Record ID → Index(metadata)

Improves lookup to O(1)

------------------------------------------------------------------------

## Compaction

Purpose: - Remove deleted records - Rebuild dataset files - Reduce disk
usage

Steps: 1. Copy active records to temp files 2. Replace original files 3.
Clear index cache

------------------------------------------------------------------------

## Performance

  Operation   Complexity
  ----------- ------------
  Insert      O(1)
  Get         O(1)
  Delete      O(1)
  Update      O(1)
  Compact     O(n)

------------------------------------------------------------------------

## Data Structure Sizes

-   Index record: 13 bytes
-   Int size: 4 bytes
-   Long size: 8 bytes

------------------------------------------------------------------------

## Design Philosophy

-   Append-only writes for safety
-   Crash tolerance via PENDING state
-   Separation of metadata and data
-   In-memory indexing for speed

------------------------------------------------------------------------

## Limitations

-   No transactions
-   No thread safety guarantees
-   Compaction not automatic
-   No checksum validation
