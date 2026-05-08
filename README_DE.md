# Tag 18 Lab -- Drei-Wege-Formatvergleich: Delta vs Hudi vs Iceberg

> :gb: [English Version](README.md)

## Ziel

Open-Table-Formate (Delta, Hudi, Iceberg) bewerten, indem Sie Delta-Primitive ausführen, ihre Tradeoffs vergleichen und eine Format-Empfehlung gegen einen *gemessenen* Workload begründen. Bloom-Stufe: **Analyze -> Create**.

Zielzeit: **unter 1 Stunde**. Die in der Vorlesung demonstrierten Primitive sind vorausgefüllt, damit die Lab-Zeit für Synthese und Bewertung bleibt -- nicht fürs Abtippen der Vorlesung.

## Lernziele

Am Ende dieses Labs können Sie:

1. **Begründen** Sie einen strukturierten `comparison_report` und **erklären** Sie, welcher Input eine `recommend_format`-Entscheidung dominiert.
2. **Implementieren** Sie `delta_delete` und `delta_history` gegen eine Delta-Tabelle und beobachten Sie das Wachstum des Transaktionslogs über die Commits write -> upsert -> delete.
3. **Kritisieren** Sie die Reihenfolge der Empfehlungsregeln -- erklären Sie, warum die Multi-Engine-Prüfung vor der Streaming-/Upsert-Prüfung kommt und was bricht, wenn Sie sie tauschen.
4. **Synthetisieren** Sie eine messwertbasierte Empfehlung in `benchmark_workload`, die die gemessenen Zahlen zitiert -- nicht nur die Regeltabelle.
5. **Bewerten** Sie die Formatwahl für einen benannten realen Fall (MedTrack), indem Sie sein Workload-Profil durch das Bewertungsraster führen und den Migrations-Trade-off explizit machen.

## Bereitgestellt

In `pipeline.py` vorausgefüllt (die Vorlesung zeigt diese vollständig -- kein Wert im Abtippen):

- `get_spark_session` -- Delta-fähige SparkSession-Factory
- `delta_write`, `delta_read`, `delta_upsert` -- die drei Delta-Primitive mit Zeitmessung
- `recommend_format` -- die Workload-zu-Format-Logik
- `comparison_report` -- der statische Drei-Wege-Vergleich (Scores in der Auswertung verteidigen)

Sie implementieren (das ist die Analyze-/Evaluate-/Create-Fläche des Labs):

- `evaluate_format_tradeoffs`
- `delta_delete`, `delta_history`
- `format_recommendation`
- `benchmark_workload` -- **beschriftete Syntheseaufgabe**
- `medtrack_recommendation`
- `run_pipeline`

Plus 36 TDD-Tests in `tests/test_format_comparison.py` für Delta-Format-Artefakte (Transaktionslog v0/v1/v2, Format-Reader-Roundtrip, Historien-Wachstum), Empfehlungslogik, Vergleichsbericht (inkl. numerische Scores), Syntheseaufgabe und MedTrack-Szenario.

## Setup

```bash
cd standalone && uv sync --dev
uv run pytest tests/ -v
```

PySpark benötigt Java 17+. Setzen Sie `JAVA_HOME`, falls es nicht im PATH liegt.

---

## Aufgaben

### :green_circle: Grundlagen

#### Aufgabe 1 -- `evaluate_format_tradeoffs` implementieren

Wickeln Sie den vorausgefüllten `comparison_report()` so, dass Aufrufer
für ein einzelnes Format das Tripel `{strengths, weaknesses, best_for}`
per Name abrufen können.

- Geben Sie ein Dict mit diesen drei Schlüsseln für `"delta"`, `"hudi"` oder `"iceberg"` zurück.
- Lösen Sie `ValueError("Unknown format: ...")` für jede andere Eingabe (z. B. `"parquet"`) aus.

Das ist die Brücke des Labs vom *Beschreiben* der Formate (Vorlesung) zum
*Beurteilen* (Lab): jede Schwäche, die Sie auflisten, und jeder Score, den
Sie im Vergleichsbericht vergeben, ist eine Bewertungs-Aussage, die Sie in
der Diskussion verteidigen müssen.

#### Aufgabe 2 -- `delta_delete` und `delta_history` implementieren

- `delta_delete` ruft `DeltaTable.forPath(spark, path).delete(condition)` auf
  und gibt `{format, remaining_rows, condition}` zurück.
- `delta_history` ruft `DeltaTable.forPath(spark, path).history().collect()`
  auf und castet jede Zeile via `row.asDict()`.

Akzeptanz: nach write -> upsert -> delete enthält `_delta_log/` die Commit-JSONs v0, v1 und v2, und `delta_history` gibt eine Liste der Länge >= 3 zurück, mit der jüngsten Operation zuerst.

### :blue_circle: Fortgeschritten

#### Aufgabe 3 -- `format_recommendation` implementieren

Wenden Sie die vorausgefüllte `recommend_format`-Logik auf vier benannte Szenarien an und geben Sie ein Dict zurück:

- `batch_single_engine` -> Delta
- `streaming_upserts` -> Hudi
- `multi_engine_batch` -> Iceberg
- `hybrid_multi_engine` -> Iceberg

#### Aufgabe 4 -- Regelordnung kritisieren (Analyze, ungeprüft)

Beantworten Sie in Ihren Notizen (kein Test prüft dies, die Diskussion schon):

- Warum prüft die Regel `multi-engine` **vor** Streaming / Upsert?
- Konstruieren Sie einen Workload, der bei vertauschter Reihenfolge die *falsche* Empfehlung erhielte. Welcher Input hat durch den Tausch die Entscheidung übernommen?

### :purple_circle: Experte

#### :red_circle: Aufgabe 5 -- Synthese: messwertbasierte Empfehlung

> **Beschriftete Syntheseaufgabe.** Die Vorlesung zeigt jedes Primitiv isoliert; hier müssen Sie drei davon kombinieren und die *Messungen* die Begründung tragen lassen.

Implementieren Sie `benchmark_workload(spark, workload_profile, path)`:

- Führen Sie `delta_write`, `delta_read` und `delta_upsert` gegen `path` aus und erfassen Sie Wall-Clock-Zeiten für jede Operation.
- Wenden Sie `recommend_format` auf die Profilfelder `primary_pattern` / `engine_diversity` / `upsert_frequency` an.
- Geben Sie ein Dict zurück mit `workload`, `write_seconds`, `read_seconds`, `upsert_seconds`, `recommended_format` und einem `justification`-String.
- Die `justification` **muss mindestens eine gemessene Zahl mit zwei Nachkommastellen zitieren** (z. B. `"upsert took 4.21s"`). Eine Begründung, die nur `comparison_report()`-Stärken zitiert, scheitert an der Synthese-Prüfung.
- Multi-Engine-Profile gehen weiterhin an Iceberg, unabhängig von gemessenen Zeiten -- Engine-Vielfalt dominiert Durchsatz.

Dies kombiniert Schreib-Benchmarking + Lese-Benchmarking + Upsert-Benchmarking + die Empfehlungslogik zu einer Entscheidung, die die Vorlesung nicht direkt demonstriert. Der Grader (`test_benchmark_workload_justification_cites_measured_numbers`) sucht in Ihrem Justification-String nach einem Zahlentoken aus Ihren Messungen.

#### Aufgabe 6 -- MedTrack-Szenario (Evaluate)

Wenden Sie das Bewertungsraster auf einen benannten realen Fall an. Bauen Sie eine `medtrack_recommendation()`,
die das MedTrack-Workload-Profil konstruiert (multi-engine durch die neuen
Trino-Lizenzen), es durch `recommend_format` führt und Deltas Schwächen aus
`comparison_report` aufzeigt, damit der Migrations-Trade-off sichtbar wird.

Warum das Evaluate ist: Sie implementieren kein neues Primitiv, sondern
*wenden* das Bewertungsraster *an* und *beurteilen* die Formatwahl für ein
konkretes Szenario. Der Grader prüft die Empfehlung; die Diskussion bewertet
Ihre `notes`.

#### Aufgabe 7 -- `run_pipeline` verdrahten

Verbinden Sie die vorausgefüllten und selbst implementierten Teile: bauen
Sie eine SparkSession, durchlaufen Sie write -> read -> upsert -> delete ->
history, loggen Sie den Vergleichsbericht und schicken Sie ein Beispiel-
Workload durch `benchmark_workload`. Kein eigener Test prüft das Verhalten
darüber hinaus -- nur "wirft nicht".

---

## Tests ausführen

```bash
uv run pytest tests/ -v
```

Alle 36 Tests müssen grün sein, wenn die Implementierung vollständig ist. Etwa die Hälfte der Testfläche ist formatspezifisch (Transaktionslog-Artefakte, MERGE-/DELETE-Versions-Bumps, Historien-Wachstum, Format-Reader-Roundtrip); der Rest übt die Empfehlungslogik, den Vergleichsbericht (inkl. numerische Scores), die Syntheseaufgabe und das MedTrack-Szenario.
