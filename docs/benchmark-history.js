(() => {
  const status = document.querySelector("#benchmark-status");
  const latestContainer = document.querySelector("#benchmark-latest");
  const select = document.querySelector("#benchmark-select");
  const chart = document.querySelector("#benchmark-chart");

  const escapeHtml = (value) => String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");

  const formatValue = (value, unit) => {
    const digits = Math.abs(value) < 0.1 ? 4 : 3;
    return `${Number(value).toFixed(digits)} ${unit}`;
  };

  const findBenchmark = (entry, name) =>
    entry.benchmarks.find((benchmark) => benchmark.name === name);

  const renderChart = (entries, name) => {
    const samples = entries
      .map((entry) => ({ entry, benchmark: findBenchmark(entry, name) }))
      .filter(({ benchmark }) => benchmark);
    if (!samples.length) {
      chart.textContent = "No samples are available for this benchmark.";
      return;
    }

    const width = 900;
    const height = 280;
    const padding = 48;
    const values = samples.map(({ benchmark }) => Number(benchmark.value));
    const low = Math.min(...values);
    const high = Math.max(...values);
    const span = high - low || Math.max(high, 1);
    const x = (index) => padding
      + index * (width - padding * 2) / Math.max(samples.length - 1, 1);
    const y = (value) => height - padding
      - (value - low) * (height - padding * 2) / span;
    const points = values.map((value, index) => `${x(index)},${y(value)}`).join(" ");
    const circles = values.map((value, index) => {
      const sample = samples[index];
      const label = `${sample.entry.commit.slice(0, 7)}: ${formatValue(value, sample.benchmark.unit)}`;
      return `<circle class="point" cx="${x(index)}" cy="${y(value)}" r="4"><title>${escapeHtml(label)}</title></circle>`;
    }).join("");
    const unit = samples.at(-1).benchmark.unit;

    chart.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(name)} history">
        <line class="axis" x1="${padding}" y1="${padding}" x2="${padding}" y2="${height - padding}"></line>
        <line class="axis" x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}"></line>
        <text x="${padding}" y="${padding - 12}">${escapeHtml(formatValue(high, unit))}</text>
        <text x="${padding}" y="${height - 14}">${escapeHtml(formatValue(low, unit))}</text>
        <text x="${padding}" y="${height - 4}">${escapeHtml(samples[0].entry.commit.slice(0, 7))}</text>
        <text x="${width - padding}" y="${height - 4}" text-anchor="end">${escapeHtml(samples.at(-1).entry.commit.slice(0, 7))}</text>
        <polyline class="series" points="${points}"></polyline>
        ${circles}
      </svg>`;
  };

  fetch("benchmark-history.json", { cache: "no-store" })
    .then((response) => {
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    })
    .then((history) => {
      const entries = history.entries || [];
      if (!entries.length) {
        status.textContent = "No tracked CI benchmark run has completed yet.";
        select.disabled = true;
        return;
      }

      const latest = entries.at(-1);
      const previous = entries.at(-2);
      const commitUrl = `https://github.com/${latest.repository}/commit/${latest.commit}`;
      status.innerHTML = `Latest run: <a href="${escapeHtml(commitUrl)}"><code>${escapeHtml(latest.commit.slice(0, 7))}</code></a>
        on ${escapeHtml(new Date(latest.timestamp).toLocaleString())} ·
        ${escapeHtml(latest.runner)} · Python ${escapeHtml(latest.python)} · Polars ${escapeHtml(latest.polars)}`;

      const rows = latest.benchmarks.map((benchmark) => {
        const old = previous && findBenchmark(previous, benchmark.name);
        const change = old && old.value
          ? `${((benchmark.value - old.value) / old.value * 100).toFixed(1)}%`
          : "—";
        return `<tr><td>${escapeHtml(benchmark.name)}</td><td>${escapeHtml(formatValue(benchmark.value, benchmark.unit))}</td><td>${escapeHtml(change)}</td></tr>`;
      }).join("");
      latestContainer.innerHTML = `
        <table class="table table-striped">
          <thead><tr><th>Benchmark</th><th>Latest</th><th>Change from previous</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;

      latest.benchmarks.forEach((benchmark) => {
        const option = document.createElement("option");
        option.value = benchmark.name;
        option.textContent = benchmark.name;
        select.append(option);
      });
      select.addEventListener("change", () => renderChart(entries, select.value));
      renderChart(entries, select.value);
    })
    .catch((error) => {
      status.textContent = `Benchmark history could not be loaded: ${error.message}`;
      select.disabled = true;
    });
})();
