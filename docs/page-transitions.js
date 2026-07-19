'use strict';

(() => {
  const PAGE_LEAVE_DURATION_MS = 260;
  let navigationPending = false;

  function resetPageTransition() {
    navigationPending = false;
    document.documentElement.classList.remove('is-page-leaving');
    document.querySelectorAll('[data-page-transition][aria-busy="true"]').forEach((link) => {
      link.removeAttribute('aria-busy');
    });
  }

  document.addEventListener('click', (event) => {
    const link = event.target.closest('a[data-page-transition]');
    if (
      !link ||
      navigationPending ||
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey ||
      link.target === '_blank' ||
      link.hasAttribute('download')
    ) {
      return;
    }

    const destination = new URL(link.href, window.location.href);
    if (destination.origin !== window.location.origin) return;

    event.preventDefault();
    navigationPending = true;
    link.setAttribute('aria-busy', 'true');
    document.documentElement.classList.add('is-page-leaving');

    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    window.setTimeout(() => {
      window.location.assign(destination.href);
    }, reducedMotion ? 0 : PAGE_LEAVE_DURATION_MS);
  });

  window.addEventListener('pageshow', resetPageTransition);
})();
