# NL2Quant - Agent Coding Guidelines

Welcome to the **NL2Quant** repository. This `AGENTS.md` file serves as the definitive manual for autonomous AI coding agents operating within this codebase. You must rigorously adhere to the rules, commands, and architectural patterns outlined below.

## 1. Repository Architecture

This is a polyglot monorepo partitioned into three core microservices:

1. **`python-engine` (AI Execution & Data Sync)**
   * Stack: Python 3.10+, FastAPI, Pydantic, DuckDB, Pandas, loguru.
   * Responsibilities: Communicates with Google GenAI (`gemini_client.py`), executes validated user-generated quant strategies (`sandbox`), and syncs financial data via AKShare (`data_sync`).

2. **`web-ui` (Frontend Client)**
   * Stack: Vue 3 (Composition API), Vite, TypeScript, Element Plus, Axios.
   * Responsibilities: Provides the user interface, rendering a white-box quant strategy builder and managing API communication with the gateway.

3. **`java-gateway` (API Gateway)**
   * Stack: Java 17+, Maven, Spring Boot (WebFlux).
   * Responsibilities: A reactive proxy routing requests from the frontend to the Python engine using `WebClient`.

**CRITICAL RULE:** Do NOT use relative paths across sub-projects in your file operation tools (`read`, `write`, `edit`). Always use absolute paths originating from the root workspace (`/Users/peiwanqiang/ai-project/my-nl2quant`).

---

## 2. Build, Lint, and Test Commands

### 2.1 Python Engine (`/python-engine`)

* **Dependencies:** Managed via `requirements.txt`. Do not assume tools like `pytest` or `ruff` exist unless explicitly listed.
* **Environment Setup:** `pip install -r requirements.txt`
* **Run Application:**
  ```bash
  cd /Users/peiwanqiang/ai-project/my-nl2quant/python-engine
  uvicorn main:app --reload --port 8000
  ```
* **Linting & Formatting:** 
  No strict auto-linter is enforced. You must manually write PEP 8 compliant code and use strict explicit Python type hints (`from typing import ...`).
* **Testing:** 
  Tests are standalone execution scripts. `pytest` is NOT the standard runner here.
  **Run a single test:**
  ```bash
  cd /Users/peiwanqiang/ai-project/my-nl2quant/python-engine
  python tests/test_macros.py
  ```

### 2.2 Web UI (`/web-ui`)

* **Dependencies:** `cd web-ui && npm install`
* **Run Development Server:** `npm run dev`
* **Build & Strict Type Check:** 
  Always run this after ANY frontend edit to verify Vue/TypeScript integrity!
  ```bash
  cd /Users/peiwanqiang/ai-project/my-nl2quant/web-ui
  npm run build
  ```
* **Testing:** No test framework (e.g., Vitest) is initialized. Do not assume `npm test` works. If testing is required, initialize Vitest first.

### 2.3 Java Gateway (`/java-gateway`)

* **Build/Compile:** 
  ```bash
  cd /Users/peiwanqiang/ai-project/my-nl2quant/java-gateway
  mvn clean compile
  ```
* **Run Application:** `mvn spring-boot:run`
* **Testing:** 
  * **Run all tests:** `mvn test`
  * **Run a single test:** `mvn -Dtest=TestClassName test` or `mvn -Dtest=TestClassName#methodName test`

---

## 3. Code Style & Engineering Conventions

### 3.1 Python Conventions (`python-engine`)
* **Logging:** **NEVER** use `print()` or the standard `logging` library. You must exclusively use `loguru`:
  ```python
  from loguru import logger
  logger.info("Informational message")
  logger.error(f"Error occurred: {e}")
  ```
* **Type Hinting & Schemas:** Annotate all functions with return types and argument types. Use `Pydantic` models (e.g., `QuantChatRequest`) for FastAPI request/response validation.
* **Data Processing (DuckDB/Pandas):** 
  * Financial data is persisted via partitioned DuckDB Parquet files. Use the provided `get_duckdb_conn()` function in `data_sync.common`.
  * When calling external market APIs (like `akshare`), you **must** use the `@retry` decorator from `tenacity` to handle network flakiness.
* **Security & Sandboxing:** Dynamically generated code from LLMs is validated via an AST whitelist (`sandbox.ast_validator`) and executed with resource limits (`sandbox.subprocess_exec`). **Never** bypass the `ALLOWED_NODES` constraints when modifying the sandbox.

### 3.2 Frontend Conventions (`web-ui`)
* **Framework:** Exclusively use the **Vue 3 Composition API** (`<script setup lang="ts">`). Do not use the Options API (`export default { data() {...} }`).
* **UI Components:** Use `Element Plus` for all generic components (Buttons, Inputs, Dialogs). Adhere to the `ElMessage` component for user-facing error notifications.
* **Typing:** Explicitly define TypeScript `interfaces` or `types` for all component props, custom events, and Axios API payloads. Minimize the usage of `any`.
* **State Management:** For global/local reactive state, use Vue's native `ref()`/`reactive()`, or composables from `@vueuse/core`.
* **Error Handling:** Wrap Axios requests in `try/catch` blocks and gracefully handle HTTP errors.

### 3.3 Java Gateway Conventions (`java-gateway`)
* **Framework:** Spring WebFlux. The application uses reactive types (`Mono`, `Flux`) and `WebClient`. **Do not** introduce blocking I/O, synchronous dependencies, or standard `RestTemplate` calls.
* **Architecture:** Maintain strict separation between Controllers, Services, and configurations.
* **Formatting:** `camelCase` for variables/methods, `PascalCase` for classes. Keep lines under 120 characters.

### 3.4 File Naming Conventions
* **Python:** `snake_case.py` for all modules, scripts, and tests (e.g., `gemini_client.py`).
* **Vue:** `PascalCase.vue` for all components (e.g., `ChatPanel.vue`).
* **TypeScript:** `camelCase.ts` or `kebab-case.ts` for utilities and composables.
* **Java:** `PascalCase.java` matching the public class name (e.g., `QuantChatController.java`).

---

## 4. Operational Directives for Agents

1. **Information Gathering:** ALWAYS use the `read` or `glob` tools to examine surrounding code, verify class names, and confirm library existence before attempting an edit. Do not guess file structures.
2. **Precision Edits:** Use the `edit` tool with exact textual matches. If an edit fails, retry with a more specific/larger block of surrounding code (`oldString`) to uniquely identify the section.
3. **No Phantom Libraries:** Do not assume common packages (like `pytest`, `lodash`, or `lombok`) are installed. Consult `requirements.txt`, `package.json`, or `pom.xml` first.
4. **Validation:** After your edits, perform a self-validation step by executing the relevant type-check or compile command (e.g., `npm run build`, `python tests/...`, `mvn compile`). Never submit broken code.
5. **Context Window Limitations:** Ensure your read requests target specific lines or rely on semantic searches/grep. Pulling entirely unpaginated data frames or massive API responses can exhaust the model’s context.
