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
          label: 'Start here',
          items: [
            { slug: 'index' },
            { slug: '00-doctrine' },
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
