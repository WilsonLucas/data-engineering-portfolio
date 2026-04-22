# Apresentação do Portfolio

## Arquivos

- [`portfolio-slides.html`](portfolio-slides.html) — Deck de 19 slides (single-file HTML).
  Abre direto no navegador. Navegação:
  - Setas do teclado (← / →)
  - Dots laterais
  - Scroll / touch
- [`SLIDE_MAP.md`](SLIDE_MAP.md) — planejamento estrutural do deck.

## Foto de perfil

O slide 1 (capa) tem um placeholder circular para foto profissional.

**Para adicionar foto:**

1. Salve sua foto como `wilson-lucas.jpg` neste diretório (`PRESENTATION/`).
2. A foto ideal é:
   - Quadrada (1:1) ou circular, 512x512 px ou maior
   - Fundo neutro (cinza claro ou branco)
   - Enquadramento chest-up (peito para cima)
   - Formato JPG ou PNG

3. Reabra o `portfolio-slides.html` — a foto aparecerá no slide 1 automaticamente (se o HTML estiver configurado para `./wilson-lucas.jpg`).

Se preferir não incluir foto, o placeholder atual (inicial "WL" em círculo navy/gold) pode permanecer — é uma escolha estilística válida para portfolio técnico.

## Exportar slides como PDF

Para gerar uma versão PDF do deck (útil para envio por e-mail ou anexo em candidaturas):

### Via Chrome/Edge headless

```bash
chrome --headless=new --disable-gpu \
  --print-to-pdf=portfolio-slides.pdf \
  --no-pdf-header-footer \
  portfolio-slides.html
```

### Via navegador manual

1. Abra `portfolio-slides.html` no Chrome
2. `Ctrl+P` → Destino: "Salvar como PDF"
3. Layout: Paisagem
4. Margens: Nenhuma
5. Desmarcar "Cabeçalhos e rodapés"

## Hospedagem

Para compartilhar o deck como link público, você pode:

1. **GitHub Pages** (recomendado — integra com o repo do portfolio):
   - Settings → Pages → Source: `main` branch / `/PRESENTATION` folder
   - URL: `https://wilsonlucas.github.io/data-engineering-portfolio/PRESENTATION/portfolio-slides.html`

2. **Vercel / Netlify** — deploy drag-and-drop do arquivo HTML.

3. **Arquivo anexo** — o HTML é single-file (117 KB), pode ir anexado a e-mails.
