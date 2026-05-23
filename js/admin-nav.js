(function () {
  var ADMIN_LINKS = [
    { href: "/mon-entreprise", label: "Mon entreprise" },
    { href: "/utilisateurs", label: "Utilisateurs" },
  ];

  function appendAdminLink(container, href, label, desktop) {
    if (container.querySelector('a[href="' + href + '"]')) return;
    var a = document.createElement("a");
    a.href = href;
    a.setAttribute("data-admin-nav-link", "1");
    if (desktop) {
      a.className =
        "px-4 py-2 rounded-md text-sm font-medium text-slate-400 transition-colors hover:bg-slate-800 hover:text-white";
    } else {
      a.className =
        "block rounded-md px-4 py-2 text-sm font-medium text-slate-600 transition-colors hover:bg-slate-100 hover:text-slate-900";
    }
    a.textContent = label;
    container.appendChild(a);
  }

  function applyNavAccess(me) {
    if (!me) return;
    if (!me.is_primary_user) {
      document.querySelectorAll('a[href="/requetes"]').forEach(function (a) {
        a.remove();
      });
    }
    if (me.role !== "admin") return;

    document.querySelectorAll("header nav").forEach(function (nav) {
      ADMIN_LINKS.forEach(function (link) {
        appendAdminLink(nav, link.href, link.label, true);
      });
    });
    document.querySelectorAll("header details.group > div").forEach(function (menu) {
      if (!menu.querySelector('a[href="/dashboard"]')) return;
      ADMIN_LINKS.forEach(function (link) {
        appendAdminLink(menu, link.href, link.label, false);
      });
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
