# Babamul web

A React + TypeScript + Vite front end for the BOOM application.

This is part of the [BOOM](https://github.com/boom-astro/boom) monorepo. There is a
**single setup process for the whole stack** (backend + frontend) — see
[**Setup** and **Start services for local development**](../README.md#setup) in the root
README. In short, from the repository root:

```bash
make dev
```

This starts the Vite dev server (with hot module reload) alongside the API and supporting
services. The web app is served at [http://localhost:5173](http://localhost:5173) and proxies
`/api` requests to the API at [http://localhost:4000](http://localhost:4000). Editing files under
`frontend/src` triggers a live rebuild in the container.

Linting and type-checking run via the repo's pre-commit hooks (`frontend-lint`, `frontend-tsc`).
To run them manually:

```bash
cd frontend
bun run lint
bun run tsc --noEmit
```

## ESLint configuration

This project uses [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react)
(Babel) for Fast Refresh. If you are expanding the ESLint configuration, we recommend enabling
type-aware lint rules:

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

You can also install [eslint-plugin-react-x](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-x)
and [eslint-plugin-react-dom](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-dom)
for React-specific lint rules:

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
