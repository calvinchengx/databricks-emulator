import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { remarkMermaid } from './plugins/remark-mermaid.mjs';

export default defineConfig({
  site: 'https://calvinchengx.github.io',
  base: '/databricks-emulator/',
  markdown: {
    remarkPlugins: [remarkMermaid],
  },
  integrations: [
    starlight({
      title: 'Databricks Emulator',
      components: {
        Head: './src/components/Head.astro',
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
            { slug: 'index' },
            { slug: '00-doctrine' },
            { slug: '01-quickstart' },
            { slug: '02-installation' },
            { slug: '03-architecture' },
            { slug: '04-configuration' },
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
          ],
        },
      ],
    }),
  ],
});
