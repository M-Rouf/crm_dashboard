(function () {
  function applyEntrepriseBrand(me) {
    if (!me || !me.entreprise_nom) return;
    if (document.getElementById("entreprise-context-bar")) return;
    var header = document.querySelector("header");
    if (!header) return;

    var nom = me.entreprise_nom;
    var bar = document.createElement("div");
    bar.id = "entreprise-context-bar";
    bar.className =
      "border-b border-slate-200 bg-slate-50/95 shadow-sm";
    bar.innerHTML =
      '<div class="container mx-auto flex max-w-7xl items-center gap-3 px-4 py-2.5 md:px-8">' +
      '<img id="entreprise-context-logo" alt="" class="hidden h-8 w-auto max-h-8 max-w-[10rem] flex-shrink-0 object-contain" />' +
      '<span id="entreprise-context-name" class="text-sm font-semibold tracking-tight text-slate-800 sm:text-base"></span>' +
      "</div>";

    header.insertAdjacentElement("afterend", bar);

    var nameEl = document.getElementById("entreprise-context-name");
    var logoEl = document.getElementById("entreprise-context-logo");
    if (nameEl) nameEl.textContent = nom;
    if (logoEl && me.entreprise_logo_url) {
      logoEl.src = me.entreprise_logo_url;
      logoEl.alt = nom + " — logo";
      logoEl.classList.remove("hidden");
    }
  }

  function applyNavAccess(me) {
    if (!me) return;
    applyEntrepriseBrand(me);
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
