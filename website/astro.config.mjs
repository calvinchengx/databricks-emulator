import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { remarkMermaid } from './plugins/remark-mermaid.mjs';

export default defineConfig({
  site: 'https://calvinchengx.github.io',
  // THE DOCS LIVE UNDER /docs/, with the hand-written landing page at the root:
  // the family's shape (data-agent-service, data-agent-voice, apim, snowflake,
  // emulators, entra). It moved 32 published routes and none of them broke --
  // scripts/assemble_site.py writes a redirect stub at every old path and holds
  // itself to website/published-routes.txt, captured from the build immediately
  // before the move.
  base: '/databricks-emulator/docs/',
  markdown: {
    remarkPlugins: [remarkMermaid],
  },
  integrations: [
    starlight({
      title: 'Databricks Emulator',
      components: {
        Head: './src/components/Head.astro',
        Search: './src/components/Search.astro',
      },
      description:
        'A local emulator of a Databricks workspace — PAT and OIDC identity, workspace files, Jobs, and an attached Spark engine — refuse what you cannot compute.',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/calvinchengx/databricks-emulator' },
      ],
      editLink: {
        baseUrl: 'https://github.com/calvinchengx/databricks-emulator/edit/main/docs/',
      },
      sidebar: [
        {
          label: 'Getting started',
          items: [
            // The docs' front door. Slug 'overview' while Starlight was
            // based at the root and index.html belonged to the landing page;
            // under /docs/ there is no collision. Starlight names the index
            // slug with an empty string.
            { slug: '' },
            { slug: '00-doctrine' },
            { slug: '01-quickstart' },
            { slug: '02-installation' },
            { slug: '03-architecture' },
            { slug: '04-configuration' },
            { slug: '21-real-databricks-toggle' },
            { slug: '05-tls-and-hosts' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { slug: '06-identity' },
            { slug: '07-workspace-and-files' },
            { slug: '08-jobs-and-spark' },
            { slug: '09-secrets' },
            { slug: '10-sql-and-mcp' },
            { slug: '11-clusters-and-connect' },
            { slug: '12-unity-catalog' },
          ],
        },
        {
          label: 'Testing',
          items: [
            { slug: '13-testing' },
            { slug: '14-family-integration' },
          ],
        },
        {
          label: 'Project',
          items: [
            { slug: '15-roadmap' },
          ],
        },
        {
          label: 'Parity',
          items: [
            { slug: 'parity', label: 'Parity ledger' },
            { slug: 'parity-history' },
            { slug: 'parity-history/changelog' },
          ],
        },
      ],
    }),
  ],
});
