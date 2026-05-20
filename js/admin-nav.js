(function () {
  function applyNavAccess(me) {
    if (!me) return;
    if (!me.is_primary_user) {
      document.querySelectorAll('a[href="/requetes"]').forEach(function (a) {
        a.remove();
      });
    }
    if (me.role !== "admin") return;
    if (
      document.querySelector("[data-admin-nav-link]") ||
      document.querySelector('a[href="/utilisateurs"]')
    ) {
      return;
    }
    document.querySelectorAll("nav.hidden.md\\:flex").forEach(function (nav) {
      var a = document.createElement("a");
      a.href = "/utilisateurs";
      a.setAttribute("data-admin-nav-link", "1");
      a.className =
        "px-4 py-2 rounded-md text-sm font-medium text-slate-400 transition-colors hover:bg-slate-800 hover:text-white";
      a.textContent = "Utilisateurs";
      nav.appendChild(a);
    });
    document.querySelectorAll("details.group > div").forEach(function (menu) {
      if (!menu.querySelector('a[href="/factures"]')) return;
      var m = document.createElement("a");
      m.href = "/utilisateurs";
      m.setAttribute("data-admin-nav-link", "1");
      m.className =
        "block rounded-md px-4 py-2 text-sm font-medium text-slate-600 transition-colors hover:bg-slate-100 hover:text-slate-900";
      m.textContent = "Utilisateurs";
      menu.appendChild(m);
    });
  }

  function initNavAccess() {
    fetch("/api/auth/me")
      .then(function (r) {
        return r.ok ? r.json() : null;
      })
      .then(applyNavAccess)
      .catch(function () {});
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initNavAccess);
  } else {
    initNavAccess();
  }
})();
