Act as a senior Enterprise Content Management (ECM) architect. I am a new graduate developer with no prior experience. I need a highly detailed, step-by-step technical guide to configure my local development environment for a specialized IBM Content Navigator (ICN) project. 

Here are the exact details, constraints, and architecture of my project:
- ICN Version: 3.1
- Repository Backend: IBM Content Manager 8 (CM8)
- Local Development App Server: WebSphere Liberty Profile in Eclipse IDE (Note: Production will use WebSphere Application Server ND).
- Project Type: A Maven-based Enterprise Java WAR file.
- Business Logic: I am developing a custom External Data Services (EDS) REST service. It acts as both the standard ICN EDS data hook (`/type/*` endpoint) for Entry Templates, and a custom REST API (`/custom/*` endpoint) triggered by JavaScript from a custom ICN desktop plugin.
- Caching Strategy: The application will pre-load DMN rules from an Oracle database table on startup using a ServletContextListener and cache them in memory using WebSphere's built-in Object Cache (`DistributedMap` via JNDI). It must include a self-healing fallback for cache misses and a background thread (`ScheduledExecutorService`) that updates the cache every 24 hours.
- Rules Engine: Operaton (the open-source community fork of Camunda 7).
- Multi-Rule Database Structure: Rules live in a single Oracle table with an active flag (`IS_ACTIVE = 'Y'`). Multiple rule sets are handled inside a single WebSphere Object Cache instance by generating a composite key formatted as `"RULE_GROUP:RULE_KEY"`.

Knowing all of this, please provide me with a comprehensive onboarding guide broken down into the following structural steps:

1. PREREQUISITES & INSTALLATION: The exact JDK version (and how to set it in Eclipse), the Eclipse Marketplace tools to download, and the local system environment variables needed for the CM8 native C++ library connectors (Information Integrator for Content / II4C).
2. DEPENDENCY MANAGEMENT: A complete Maven `pom.xml` pre-configured with dependencies for the Java Servlet API, the WebSphere DistributedMap caching library, Jackson databind for nested JSON parsing, and the official Operaton DMN engine coordinates.
3. WEBSPHERE LIBERTY CONFIGURATION: The complete XML blocks to add to my local `server.xml` file, including the required `<featureManager>` tags, the `<distributedMap>` cache definition, and the `<webApplication>` entry pointing to my Maven target directory.
4. SYSTEM ARCHITECTURE BEST PRACTICES: Provide practical advice specifically tailored for a junior developer, such as how to configure ICN Administrator "Has Dependent" flags to prevent excessive HTTP traffic, running Eclipse as administrator, and connecting to the company VPN.

Please write the response using short, simple, universal language. Avoid assumptions and provide clear structural guidelines so I can set everything up without getting stuck.
