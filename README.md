# Babamul web

A React + TypeScript + Vite front end for the BOOM application.

## Local development

### Prerequisites

- Docker and Docker Compose installed
- Environment variables configured (see below)

### Setup

1. Clone the front end repo:

   ```bash
   git clone https://github.com/boom-astro/babamul-web.git
   cd babamul-web
   ```

2. Create a `.env` file for the front end:
   ```bash
   cp .env.example .env
   ```

3. Repeat the first two steps for the backend services
   (https://github.com/boom-astro/boom)
   and spin them up for local development:
   ```bash
   make dev
   ```

4. Build and start the front end development container:
   ```bash
   docker-compose up --build
   ```
   The app will be available at `http://localhost:5173`

## Developer Notes

### Pre-commit hook

Install our pre-commit hook:

```bash
pre-commit install
```

This will check your changes before each commit to ensure that they conform with our code style standards.

### Notes

- `docker-compose.override.yaml` is automatically applied for local development
  - It exposes port 5173 for local access
  - It uses a local network instead of requiring an external Traefik network
- For production deployments, only `docker-compose.yaml` is used by
  explicitly specifying it with `docker compose -f docker-compose.yaml up`
- Do not commit local `.env` files to version control

## Template info

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Babel](https://babeljs.io/) for Fast Refresh
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/) for Fast Refresh

### Expanding the ESLint configuration

If you are developing a production application, we recommend updating the configuration to enable type-aware lint rules:

```js
export default tseslint.config({
  extends: [
    // Remove ...tseslint.configs.recommended and replace with this
    ...tseslint.configs.recommendedTypeChecked,
    // Alternatively, use this for stricter rules
    ...tseslint.configs.strictTypeChecked,
    // Optionally, add this for stylistic rules
    ...tseslint.configs.stylisticTypeChecked,
  ],
  languageOptions: {
    // other options...
    parserOptions: {
      project: ['./tsconfig.node.json', './tsconfig.app.json'],
      tsconfigRootDir: import.meta.dirname,
    },
  },
})
```

You can also install [eslint-plugin-react-x](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-x) and [eslint-plugin-react-dom](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-dom) for React-specific lint rules:

```js
// eslint.config.js
import reactX from 'eslint-plugin-react-x'
import reactDom from 'eslint-plugin-react-dom'

export default tseslint.config({
  plugins: {
    // Add the react-x and react-dom plugins
    'react-x': reactX,
    'react-dom': reactDom,
  },
  rules: {
    // other rules...
    // Enable its recommended typescript rules
    ...reactX.configs['recommended-typescript'].rules,
    ...reactDom.configs.recommended.rules,
  },
})
```
