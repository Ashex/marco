// What `marco serve --public-url` puts in the page's head: the API listener's
// public origin, or '' (a dev server, or no flag).
export const publicUrl = document.querySelector<HTMLMetaElement>('meta[name="marco-public-url"]')?.content ?? ''
