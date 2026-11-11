import { createElement } from 'react'
import { renderToReadableStream } from 'react-dom/server'
import PageHeader from '../../web/src/shared/Management/PageHeader.tsx'

/** Stream fixture rows through React SSR with the shared header; this workload deliberately excludes routing and authentication. */
export async function renderIsolationRows(
    rows: readonly { id: string; name: string; enabled: boolean }[],
) {
    const header = createElement(PageHeader, {
        eyebrow: 'Isolation fixture',
        title: 'Proxy hosts',
        description:
            'Real React streaming SSR; fixture rows and no application routing/authentication.',
    })
    const table = createElement(
        'table',
        null,
        createElement(
            'tbody',
            null,
            ...rows.map((row) =>
                createElement(
                    'tr',
                    { key: row.id },
                    createElement('td', null, row.id),
                    createElement('td', null, row.name),
                    createElement('td', null, row.enabled ? 'enabled' : 'disabled'),
                ),
            ),
        ),
    )
    return new Response(await renderToReadableStream(createElement('main', null, header, table)), {
        headers: { 'Content-Type': 'text/html' },
    })
}
