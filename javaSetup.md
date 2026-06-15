# IBM Content Navigator Developer Environment Setup Guide

## Overview

This guide covers the complete local development environment for IBM Content Navigator (ICN) plugin development and External Data Services (EDS) for WebSphere. All products are installed under `C:\products`. The developer deploys to a shared test server for integration testing.

---

## Directory Structure

All tooling lives under `C:\products` with the following layout:

```
C:\products\
â”œâ”€â”€ binaries\                   # Downloaded installers and ZIPs (keep for re-install)
â”‚   â”œâ”€â”€ eclipse\
â”‚   â”œâ”€â”€ jdk\
â”‚   â”œâ”€â”€ maven\
â”‚   â”œâ”€â”€ liberty\
â”‚   â”œâ”€â”€ nodejs\
â”‚   â””â”€â”€ git\
â”œâ”€â”€ eclipse\                    # Eclipse IDE installation
â”œâ”€â”€ jdk\                        # Java Development Kit
â”‚   â””â”€â”€ temurin-8\
â”œâ”€â”€ maven\                      # Apache Maven
â”œâ”€â”€ liberty\                    # Open Liberty runtime (local dev server)
â”‚   â””â”€â”€ wlp\
â”œâ”€â”€ nodejs\                     # Node.js runtime
â”œâ”€â”€ git\                        # Git installation
â””â”€â”€ workspace\                  # Eclipse workspace root
    â”œâ”€â”€ icn-plugins\            # ICN plugin Maven projects
    â””â”€â”€ eds-services\           # EDS WAR Maven projects
```

> **Convention:** `C:\products\binaries\<tool>\` holds the original ZIP or installer so the environment can be rebuilt without re-downloading.

---

## 1. Java Development Kit (JDK)

### Download
- **Distribution:** Eclipse Temurin (OpenJDK) â€” free, no Oracle licence required
- **Version:** JDK 8 (align with your ICN server version; add JDK 17 as a secondary if required)
- **Download URL:** https://adoptium.net/temurin/releases/?version=8
- **Save installer to:** `C:\products\binaries\jdk\`
- **Install to:** `C:\products\jdk\temurin-8\`

### Environment Variables
Add the following to Windows System Environment Variables:

| Variable | Value |
|---|---|
| `JAVA_HOME` | `C:\products\jdk\temurin-8` |
| `PATH` (append) | `%JAVA_HOME%\bin` |

### Verify
```cmd
java -version
javac -version
```

---

## 2. Apache Maven

### Download
- **Version:** Latest 3.9.x release
- **Download URL:** https://maven.apache.org/download.cgi (Binary zip archive)
- **Save ZIP to:** `C:\products\binaries\maven\`
- **Extract to:** `C:\products\maven\` (so the result is `C:\products\maven\bin\mvn`)

### Environment Variables

| Variable | Value |
|---|---|
| `MAVEN_HOME` | `C:\products\maven` |
| `PATH` (append) | `%MAVEN_HOME%\bin` |

### Local Repository
Maven's local repository caches downloaded JARs. By default it goes to `C:\Users\<username>\.m2\repository`. This is fine to leave as-is.

### IBM JARs (Not in Maven Central)
IBM's `navigatorAPI.jar` and EDS plugin JARs are **not published to Maven Central** and must be installed into the local repository manually. Once the ICN server is accessible, copy these JARs from the ICN installation and run:

```cmd
mvn install:install-file ^
  -Dfile=C:\path\to\navigatorAPI.jar ^
  -DgroupId=com.ibm.ecm ^
  -DartifactId=navigatorAPI ^
  -Dversion=3.0.15 ^
  -Dpackaging=jar

mvn install:install-file ^
  -Dfile=C:\path\to\edsPlugin.jar ^
  -DgroupId=com.ibm.ecm ^
  -DartifactId=edsPlugin ^
  -Dversion=3.0.15 ^
  -Dpackaging=jar
```

After installing, reference these in `pom.xml` as:
```xml
<dependency>
  <groupId>com.ibm.ecm</groupId>
  <artifactId>navigatorAPI</artifactId>
  <version>3.0.15</version>
  <scope>provided</scope>
</dependency>
```

> Use `<scope>provided</scope>` â€” these JARs are supplied at runtime by the ICN server and must **not** be bundled into the output JAR/WAR.

### Verify
```cmd
mvn -version
```

---

## 3. Git

### Download
- **Download URL:** https://git-scm.com/download/win
- **Save installer to:** `C:\products\binaries\git\`
- **Install to:** `C:\products\git\`
- During installation, set **default editor** to VS Code and select **"Git from the command line and also from 3rd-party software"**

### Environment Variables
The Git installer handles PATH automatically if the install path is set correctly. Verify with:
```cmd
git --version
```

---

## 4. Eclipse IDE

### Download
- **Edition:** Eclipse IDE for Enterprise Java and Web Developers
- **Download URL:** https://www.eclipse.org/downloads/packages/
- **Save ZIP to:** `C:\products\binaries\eclipse\`
- **Extract to:** `C:\products\eclipse\` (so `eclipse.exe` is at `C:\products\eclipse\eclipse.exe`)

### Configure Eclipse to Use the Correct JDK
1. Open `C:\products\eclipse\eclipse.ini`
2. Add the following two lines **before** `-vmargs`:
```
-vm
C:/products/jdk/temurin-8/bin/javaw.exe
```

### Workspace
Set the Eclipse workspace to `C:\products\workspace` on first launch.

### Install IBM WebSphere Developer Tools
1. In Eclipse: **Help â†’ Eclipse Marketplace**
2. Search: `IBM Liberty Developer Tools`
3. Install **IBM WebSphere Application Server Liberty Developer Tools**
4. Restart Eclipse when prompted

This adds Liberty server management directly into the Eclipse Servers view â€” start, stop, publish, and debug the local Liberty server without leaving the IDE.

### Install IBM ICN Eclipse Plugin JARs
These JARs add ICN-specific project wizards and facets to Eclipse.

1. Download the sample plugin JARs from: https://github.com/ibm-ecm/ibm-content-navigator-samples/tree/master/eclipsePlugin
2. Copy the JAR files into:
   ```
   C:\products\eclipse\dropins\
   ```
3. Restart Eclipse
4. Verify by checking **File â†’ New â†’ Project** â€” you should see an **IBM Content Navigator** category

### Install GitHub Copilot for Eclipse
1. **Help â†’ Eclipse Marketplace**
2. Search: `GitHub Copilot`
3. Install and sign in with your GitHub account that has the Copilot licence

---

## 5. Open Liberty (Local Development Server)

Open Liberty is the free, open-source runtime used for local development and hot-reload testing. The developer tests integration by deploying to the shared test server; Open Liberty handles unit-level local testing during active development.

### Download
- **Download URL:** https://openliberty.io/downloads/ â€” select **"Kernel"** zip (smallest footprint; features added on demand)
- **Save ZIP to:** `C:\products\binaries\liberty\`
- **Extract to:** `C:\products\liberty\` (resulting in `C:\products\liberty\wlp\`)

### Create a Development Server
Open a command prompt and run:

```cmd
cd C:\products\liberty\wlp\bin
server create icndev
```

This creates the server instance at:
```
C:\products\liberty\wlp\usr\servers\icndev\
```

### Server Directory Structure
```
C:\products\liberty\wlp\usr\servers\icndev\
â”œâ”€â”€ server.xml              # Main server configuration
â”œâ”€â”€ server.env              # Environment variables for this server
â”œâ”€â”€ jvm.options             # JVM tuning options
â”œâ”€â”€ apps\                   # Applications deployed here (WAR/JAR)
â”œâ”€â”€ dropins\                # Auto-deploy directory (drop WAR/JAR here to deploy)
â””â”€â”€ logs\                   # Server logs (console.log, messages.log)
```

### Configure `server.xml`
Edit `C:\products\liberty\wlp\usr\servers\icndev\server.xml` to enable the features needed for ICN plugin and EDS development:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<server description="ICN Development Server">

    <featureManager>
        <feature>servlet-4.0</feature>
        <feature>jndi-1.0</feature>
        <feature>jdbc-4.2</feature>
        <feature>localConnector-1.0</feature>
        <feature>restConnector-2.0</feature>
    </featureManager>

    <!-- HTTP endpoint -->
    <httpEndpoint id="defaultHttpEndpoint"
                  host="*"
                  httpPort="9080"
                  httpsPort="9443" />

    <!-- Application deployment â€” EDS WAR example -->
    <application id="myEDS"
                 location="my-eds-service.war"
                 name="myEDS"
                 context-root="/eds"
                 type="war" />

    <!-- Enable loose config for Maven hot-reload -->
    <applicationMonitor updateTrigger="mbean" />

</server>
```

> For EDS development, the `context-root` value (e.g. `/eds`) must match the URL registered in the ICN admin console under **External Data Services**.

### Configure `server.env`
Edit `C:\products\liberty\wlp\usr\servers\icndev\server.env` to set the Java home:

```
JAVA_HOME=C:/products/jdk/temurin-8
```

### Start / Stop the Server
```cmd
C:\products\liberty\wlp\bin\server start icndev
C:\products\liberty\wlp\bin\server stop icndev
C:\products\liberty\wlp\bin\server run icndev     # Foreground â€” shows console output
```

### Add Liberty Server to Eclipse
1. In Eclipse: **Window â†’ Show View â†’ Servers**
2. Right-click in Servers view â†’ **New â†’ Server**
3. Select **IBM â†’ WebSphere Application Server Liberty**
4. Point it at `C:\products\liberty\wlp`
5. Select the `icndev` server instance
6. The server can now be started, stopped, and used for publishing directly from Eclipse

---

## 6. Maven Project Setup for ICN Plugin

### Project Structure (JAR output)
```
icn-my-plugin/
â”œâ”€â”€ pom.xml
â””â”€â”€ src/
    â””â”€â”€ main/
        â”œâ”€â”€ java/
        â”‚   â””â”€â”€ com/yourcompany/icn/
        â”‚       â””â”€â”€ MyPlugin.java
        â””â”€â”€ resources/
            â””â”€â”€ com/yourcompany/icn/
                â””â”€â”€ MyPlugin.properties
```

### `pom.xml` Template â€” ICN Plugin
```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.yourcompany</groupId>
  <artifactId>icn-my-plugin</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <dependencies>
    <!-- IBM ICN API â€” installed to local repo manually -->
    <dependency>
      <groupId>com.ibm.ecm</groupId>
      <artifactId>navigatorAPI</artifactId>
      <version>3.0.15</version>
      <scope>provided</scope>
    </dependency>
    <!-- Java EE Servlet API â€” provided by Liberty -->
    <dependency>
      <groupId>jakarta.servlet</groupId>
      <artifactId>jakarta.servlet-api</artifactId>
      <version>4.0.4</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.12.1</version>
        <configuration>
          <source>8</source>
          <target>8</target>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <version>3.3.0</version>
      </plugin>
    </plugins>
  </build>
</project>
```

### Build Output
```cmd
cd C:\products\workspace\icn-plugins\icn-my-plugin
mvn clean package
```
Output JAR: `target\icn-my-plugin-1.0.0-SNAPSHOT.jar`

---

## 7. Maven Project Setup for EDS (WAR output)

### `pom.xml` Template â€” EDS Service
```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.yourcompany</groupId>
  <artifactId>my-eds-service</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <packaging>war</packaging>

  <dependencies>
    <dependency>
      <groupId>com.ibm.ecm</groupId>
      <artifactId>edsPlugin</artifactId>
      <version>3.0.15</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>jakarta.servlet</groupId>
      <artifactId>jakarta.servlet-api</artifactId>
      <version>4.0.4</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.12.1</version>
        <configuration>
          <source>8</source>
          <target>8</target>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-war-plugin</artifactId>
        <version>3.4.0</version>
      </plugin>
      <!-- Liberty Maven Plugin â€” enables mvn liberty:dev hot-reload -->
      <plugin>
        <groupId>io.openliberty.tools</groupId>
        <artifactId>liberty-maven-plugin</artifactId>
        <version>3.10</version>
        <configuration>
          <serverName>icndev</serverName>
          <installDirectory>C:/products/liberty/wlp</installDirectory>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

### Hot-Reload Dev Mode (Local Liberty)
```cmd
mvn liberty:dev
```
Changes to Java source files are automatically compiled and the WAR is redeployed to the local Liberty server without a restart. Press `Enter` in the terminal to run tests; press `Ctrl+C` to exit dev mode.

---

## 8. Deploying to the Shared Test Server

Since the shared test server runs a full WebSphere (traditional or Liberty) instance with ICN installed, deployment is done by **copying the built artifact** to the server.

### ICN Plugin Deployment
1. Build the plugin JAR:
   ```cmd
   mvn clean package
   ```
2. Copy `target\icn-my-plugin-1.0.0-SNAPSHOT.jar` to the test server's ICN plugin directory (coordinate path with the server admin â€” typically under the ICN web application's `plugins\` folder or a configured plugin path)
3. Log into the **ICN Administration Desktop** â†’ **Plug-ins** â†’ select the plugin â†’ click **Reload** (or register it if first time)

### EDS WAR Deployment
1. Build the WAR:
   ```cmd
   mvn clean package
   ```
2. Copy `target\my-eds-service.war` to the test server's WebSphere `dropins\` directory, or use the WebSphere Admin Console to deploy/update the application
3. Verify the EDS endpoint responds:
   ```
   GET http://<testserver>:<port>/eds/types
   ```
4. In the ICN Admin Desktop â†’ **External Data Services** â†’ confirm the registered URL points to the test server endpoint

### Recommended Workflow
```
Code change in Eclipse
        â†“
mvn clean package
        â†“
Copy JAR/WAR to test server manually (or via a script)
        â†“
Reload plugin / redeploy WAR on test server
        â†“
Verify in ICN UI on test server
```

> A simple `deploy.bat` script in the project root can automate the copy step â€” parameterise it with the test server share path so the developer runs `deploy.bat` after each build.

---

## 9. Visual Studio Code

Used primarily for JavaScript/Dojo frontend work within ICN plugins, and for editing configuration files.

### Download
- **Download URL:** https://code.visualstudio.com/
- **Save installer to:** `C:\products\binaries\`
- **Install to:** default location (`C:\Users\<username>\AppData\Local\Programs\Microsoft VS Code`) or redirect to `C:\products\vscode\` during installation

### Extensions to Install
- **GitHub Copilot** â€” sign in with the licensed GitHub account
- **Extension Pack for Java** â€” Java language support, Maven integration
- **XML** (Red Hat) â€” for editing `server.xml`, `pom.xml`, ICN configuration XML
- **REST Client** â€” for testing EDS endpoints inline without leaving VS Code

---

## 10. Node.js

Required for Dojo frontend tooling and any JavaScript build steps in ICN plugin UI development.

### Download
- **Version:** LTS (currently 20.x)
- **Download URL:** https://nodejs.org/en/download
- **Save installer to:** `C:\products\binaries\nodejs\`
- **Install to:** `C:\products\nodejs\`

### Environment Variables

| Variable | Value |
|---|---|
| `PATH` (append) | `C:\products\nodejs` |

### Verify
```cmd
node --version
npm --version
```

---

## 11. Postman (Free Tier)

Used to test EDS REST endpoints during development.

### Download
- **Download URL:** https://www.postman.com/downloads/
- Install using default settings

### EDS Endpoint Testing
The EDS REST protocol defines a standard set of endpoints. Key ones to test:

| Endpoint | Method | Description |
|---|---|---|
| `/types` | GET | Returns all document types the EDS handles |
| `/types/{type}/requestedData` | POST | Returns field definitions and values for a given type |
| `/types/{type}/externalDataSearch` | POST | Performs an external search for property values |

Test these against both the local Liberty server (`http://localhost:9080/eds/...`) and the shared test server to confirm parity.

---

## Environment Variables Summary

| Variable | Value |
|---|---|
| `JAVA_HOME` | `C:\products\jdk\temurin-8` |
| `MAVEN_HOME` | `C:\products\maven` |
| `PATH` | `%JAVA_HOME%\bin;%MAVEN_HOME%\bin;C:\products\nodejs;C:\products\git\bin` |

---

## Quick Reference: Key Commands

| Task | Command |
|---|---|
| Build ICN plugin JAR | `mvn clean package` |
| Build EDS WAR | `mvn clean package` |
| Start local Liberty server | `C:\products\liberty\wlp\bin\server start icndev` |
| Hot-reload dev mode (EDS) | `mvn liberty:dev` |
| Install IBM JAR to local repo | `mvn install:install-file -Dfile=... -DgroupId=... -DartifactId=... -Dversion=... -Dpackaging=jar` |
| Verify Java | `java -version` |
| Verify Maven | `mvn -version` |
| Verify Node | `node --version` |