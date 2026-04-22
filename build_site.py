"""
Converte case studies e technical notes de Markdown para HTML estilizado.
Usa template comum + CSS compartilhado (assets/style.css).
"""
from pathlib import Path
import markdown
import re

ROOT = Path(__file__).parent

# (md_source, html_output, title, section_label, back_link, prev_link, next_link)
PAGES = [
    # Case Studies
    (
        "case-studies/01-controller-driven-medallion.md",
        "case-studies/01-controller-driven-medallion.html",
        "Case 01 · Controller-driven Medallion Architecture",
        "Case Study 01 · Financial Services",
        ("../index.html#cases", "Voltar ao portfolio"),
        None,
        ("02-folha-pagamento-40M-linhas.html", "Case 02 · Folha de Pagamento →"),
    ),
    (
        "case-studies/02-folha-pagamento-40M-linhas.md",
        "case-studies/02-folha-pagamento-40M-linhas.html",
        "Case 02 · Folha de Pagamento · 40M+ Linhas",
        "Case Study 02 · Healthcare",
        ("../index.html#cases", "Voltar ao portfolio"),
        ("01-controller-driven-medallion.html", "← Case 01 · Controller-driven"),
        ("03-homologacao-byte-a-byte.html", "Case 03 · Homologação Byte-a-Byte →"),
    ),
    (
        "case-studies/03-homologacao-byte-a-byte.md",
        "case-studies/03-homologacao-byte-a-byte.html",
        "Case 03 · Homologação Byte-a-Byte",
        "Case Study 03 · Setor Público",
        ("../index.html#cases", "Voltar ao portfolio"),
        ("02-folha-pagamento-40M-linhas.html", "← Case 02 · Folha de Pagamento"),
        ("04-migracao-camada-semantica.html", "Case 04 · Migração Camada Semântica →"),
    ),
    (
        "case-studies/04-migracao-camada-semantica.md",
        "case-studies/04-migracao-camada-semantica.html",
        "Case 04 · Migração de Camada Semântica",
        "Case Study 04 · Corporativo",
        ("../index.html#cases", "Voltar ao portfolio"),
        ("03-homologacao-byte-a-byte.html", "← Case 03 · Homologação Byte-a-Byte"),
        None,
    ),
    # Technical Notes
    (
        "technical-notes/controller-driven-architecture.md",
        "technical-notes/controller-driven-architecture.html",
        "Padrão · Controller-driven Architecture",
        "Padrão Técnico 01",
        ("../index.html#padroes", "Voltar ao portfolio"),
        None,
        ("dynamic-partition-overwrite.html", "Dynamic Partition Overwrite →"),
    ),
    (
        "technical-notes/dynamic-partition-overwrite.md",
        "technical-notes/dynamic-partition-overwrite.html",
        "Padrão · Dynamic Partition Overwrite",
        "Padrão Técnico 02",
        ("../index.html#padroes", "Voltar ao portfolio"),
        ("controller-driven-architecture.html", "← Controller-driven"),
        ("homologacao-metodologia.html", "Homologação Zero-Amostragem →"),
    ),
    (
        "technical-notes/homologacao-metodologia.md",
        "technical-notes/homologacao-metodologia.html",
        "Padrão · Homologação Zero-Amostragem",
        "Padrão Técnico 03",
        ("../index.html#padroes", "Voltar ao portfolio"),
        ("dynamic-partition-overwrite.html", "← Dynamic Partition Overwrite"),
        None,
    ),
    # Bio
    (
        "SOBRE.md",
        "SOBRE.html",
        "Sobre · Wilson Lucas",
        "Bio Expandida",
        ("index.html", "Voltar ao portfolio"),
        None,
        None,
    ),
]

NAVBAR = """<nav class="navbar">
    <div class="navbar-inner">
        <div class="navbar-brand"><a href="{home}" style="color: inherit;">Wilson Lucas · <em>Portfolio</em></a></div>
        <div class="navbar-links">
            <a href="{home}#sobre">Sobre</a>
            <a href="{home}#stack">Stack</a>
            <a href="{home}#cases">Case Studies</a>
            <a href="{home}#padroes">Padrões</a>
            <a href="{home}#contato">Contato</a>
        </div>
    </div>
</nav>"""

TEMPLATE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="{title} — Portfolio de Engenharia de Dados de Wilson Lucas.">
<title>{title} | Wilson Lucas · Portfolio</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Newsreader:ital,wght@0,400;0,500;1,400;1,500&family=Inter:wght@400;500;600;700&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
<link rel="stylesheet" href="{css_path}">
</head>
<body>

{navbar}

<header class="article-header">
    <div class="container">
        <a href="{back_href}" class="back-link">{back_label}</a>
        <div class="section-label">// {section_label}</div>
    </div>
</header>

<article class="container">
    <div class="article-content">
{body}
    </div>

    {nav}
</article>

<footer>
    <div class="container">
        <p><strong>Wilson Lucas</strong> · Engenheiro de Dados Sênior</p>
        <p><em>Material conceitual e anonimizado · Nenhum dado de cliente exposto</em></p>
    </div>
</footer>

</body>
</html>
"""

def build_nav(prev_link, next_link):
    if not prev_link and not next_link:
        return ""
    prev_html = f'<a href="{prev_link[0]}">{prev_link[1]}</a>' if prev_link else '<span></span>'
    next_html = f'<a href="{next_link[0]}">{next_link[1]}</a>' if next_link else '<span></span>'
    return f'<nav class="article-nav">{prev_html}{next_html}</nav>'

def fix_md_links(body_html):
    """Converte links .md em links .html internos."""
    body_html = re.sub(r'href="([^"]+)\.md"', r'href="\1.html"', body_html)
    # Links relativos que apontam para outros diretórios do portfolio
    body_html = re.sub(r'href="\.\./([a-z-]+)/([^"]+)\.html"', r'href="../\1/\2.html"', body_html)
    return body_html

def build_page(md_source, html_output, title, section_label, back_link, prev_link, next_link):
    md_path = ROOT / md_source
    out_path = ROOT / html_output

    md_text = md_path.read_text(encoding="utf-8")

    # Remove the first H1 (we use the title from the tuple, not the MD H1, to avoid duplication)
    # Actually, keep it — it's useful as page heading in the article body
    body_html = markdown.markdown(
        md_text,
        extensions=["extra", "sane_lists", "smarty", "tables", "fenced_code", "codehilite"],
    )
    body_html = fix_md_links(body_html)

    # Compute relative paths based on nesting
    depth = html_output.count("/")
    home = "../" * depth + "index.html" if depth > 0 else "index.html"
    css_path = "../" * depth + "assets/style.css" if depth > 0 else "assets/style.css"

    navbar = NAVBAR.format(home=home)
    nav_html = build_nav(prev_link, next_link)

    html = TEMPLATE.format(
        title=title,
        css_path=css_path,
        navbar=navbar,
        back_href=back_link[0],
        back_label=back_link[1],
        section_label=section_label,
        body=body_html,
        nav=nav_html,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path

def main():
    print(f"Building site in {ROOT}\n")
    for page in PAGES:
        out = build_page(*page)
        rel = out.relative_to(ROOT)
        size_kb = out.stat().st_size / 1024
        print(f"  [OK] {rel}  ({size_kb:.1f} KB)")
    print(f"\nDone. {len(PAGES)} pages generated.")

if __name__ == "__main__":
    main()
