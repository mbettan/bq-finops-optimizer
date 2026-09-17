/**
 * Pre-paint theme bootstrap.
 *
 * Must run BEFORE first paint, otherwise the user sees a flash of the wrong
 * theme. This lives in its own file rather than an inline <script> because the
 * app's CSP sets `script-src 'self'` with no 'unsafe-inline' — an inline
 * bootstrap would be blocked outright.
 *
 * Load it synchronously in <head>, before the stylesheet:
 *     <script src="./static/theme-boot.js"></script>
 *
 * The class goes on <html>, not <body>, because <body> does not exist yet
 * while <head> is being parsed. Custom properties inherit downward, so tokens
 * declared on <html> are visible everywhere.
 */
(function () {
    'use strict';

    var STORAGE_KEY = 'bq_theme';
    var stored = null;

    try {
        stored = window.localStorage.getItem(STORAGE_KEY);
    } catch (e) {
        // Private browsing or blocked storage — fall back to OS preference.
        stored = null;
    }

    var theme = stored === 'light' || stored === 'dark' ? stored : null;

    if (!theme) {
        // No explicit choice yet: follow the OS. Dark remains the product
        // default, so only an explicit light preference flips it.
        var prefersLight = window.matchMedia &&
            window.matchMedia('(prefers-color-scheme: light)').matches;
        theme = prefersLight ? 'light' : 'dark';
    }

    var root = document.documentElement;
    root.classList.remove('dark-theme', 'light-theme');
    root.classList.add(theme === 'light' ? 'light-theme' : 'dark-theme');
})();
