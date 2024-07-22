
<p align="center">
  <h2 align="center">Quorum-based Total Order Broadcast System</h2>
  <p align="center">Alessandro Rizzetto - Taras Rashkevych</p>
</p>

# Table of Contents

- [Table of Contents](#table-of-contents)
- [Description](#description)
- [Features](#features)
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Usage](#usage)


# Description

This project implements a quorum-based total order broadcast system using Akka. It ensures all replicas within a distributed system can handle updates and apply them in a consistent order, despite potential node failures.

# Features

- Two-phase broadcast protocol for update management.
- Resilience to node failures with dynamic coordinator election.
- Implements read and write operations on replicas with sequential consistency guaranteed.

# System Requirements

- Java 11 or higher
- Akka 2.6
- Gradle 7 or newer (for building and managing dependencies)

# Installation

Clone this repository and build the project using Gradle:

```bash
git clone hhttps://github.com/AlessandroRizzetto/distributed-systems.git
cd distributed-systems
gradle build
```

# Usage

Run the system using:

```bash
gradle run
```

Test the system using:

```bash
gradle test
```
