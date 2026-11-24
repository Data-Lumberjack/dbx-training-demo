# Pokaz: bundle sales_etl (lekcje 5.2–5.4)

Jeden bundle, trzy targety w jednym workspace szkolenia. Każdy target pisze do swojego schematu w katalogu `szkolenie`.

## Co robi projekt
Job `sales_job` ma trzy zadania, jedno po drugim:
1. `ingest_orders` (notebook na klastrze jobu) dopisuje do tabeli `raw_orders` partię 204 zamówień z dnia
   `run_date`. Cztery wiersze są celowo błędne.
2. `refresh_pipeline` odświeża pipeline `sales_pipeline` (serverless):
   - `orders_clean`: tabela strumieniowa z nowymi wierszami `raw_orders`. Reguły `has_customer` i `positive_quantity`
     odrzucają 3 błędne wiersze, a `known_region` przepuszcza wiersz z regionem Szczecin i tylko go liczy;
   - `daily_sales` i `category_sales`: widoki zmaterializowane ze sprzedażą na dzień oraz na dzień i kategorię.
3. `check_quality` (notebook na klastrze jobu) sprawdza partię z tego runu i `daily_sales`. Gdy warunek nie jest
   spełniony, run kończy się błędem.

Parametr `run_date` ma domyślnie dzień startu runu (UTC). Harmonogram: codziennie o 6:00 czasu polskiego, w dev
i test wstrzymany przez CLI.

| Target | Tryb | Schemat | Klaster jobu | Kto uruchamia job i pipeline |
|---|---|---|---|---|
| dev (domyślny) | development | demo_dev | 1 węzeł, 4 vCPU | osoba, która wdraża |
| test | development | demo_test | 1 węzeł, 4 vCPU | osoba, która wdraża |
| prod | production | demo_prod | sterownik i 1 worker, 8 vCPU | service principal szkolenie-etl |

Każdy target wdraża się do folderu domowego osoby, która wdraża: `/Workspace/Users/<login>/.bundle/sales_etl/<target>`.

Pliki bundle'a są tylko na gałęzi `main`. Gałąź `feature-x` służy pokazowi 5.1 (folder Git). Po scaleniu pull
requestu z `feature-x` nie usuwaj tej gałęzi, bo pokaz 5.1 używa jej w każdej turze.

## Pliki
- `databricks.yml`: nazwa bundle'a, zmienne (`catalog`, `schema`, `job_cluster`, `etl_sp`) i targety.
- `resources/sales_job.yml`: job `sales_job` z parametrami, klastrem jobu i harmonogramem.
- `resources/sales_pipeline.yml`: pipeline `sales_pipeline`.
- `src/notebooks/ingest_orders.py`, `src/notebooks/check_quality.py`: notebooki zadań joba.
- `src/pipeline/*.sql`: kod pipeline'u, jeden plik na tabelę.

## Raz przed pokazem
1. Profil CLI: `databricks auth login --host <adres workspace'u> --profile prowadzacy`.
   `databricks current-user me -p prowadzacy` pokazuje Twój login.
2. `databricks bundle validate -t prod -p prowadzacy` kończy się `Validation OK!`.
3. `szkolenie-etl` ma **Can Use** na **Default Automated Compute** (**Compute** → **Serverless**). Bez tego pipeline
   w prod kończy się błędem `COST_CONTROL_ENTITLEMENT_DENIED`.
4. Próba całości: polecenia pokazu poniżej, potem sprzątanie z części „Po pokazie”.

## Polecenia pokazu
Terminal VS Code (PowerShell), folder repo, gałąź `main`.

```powershell
# 5.2: zmienne i targety
databricks bundle validate -t dev -p prowadzacy
# schemat pipeline'u po rozwiązaniu zmiennych: demo_dev
(databricks bundle validate -t dev -p prowadzacy -o json | Out-String | ConvertFrom-Json).resources.pipelines.sales_pipeline.schema
# --var wygrywa z wartością z targetu: demo_test
(databricks bundle validate -t dev -p prowadzacy --var="schema=demo_test" -o json | Out-String | ConvertFrom-Json).resources.pipelines.sales_pipeline.schema
# to samo przez zmienną środowiskową BUNDLE_VAR_<nazwa zmiennej>: demo_test
$env:BUNDLE_VAR_schema = "demo_test"
(databricks bundle validate -t dev -p prowadzacy -o json | Out-String | ConvertFrom-Json).resources.pipelines.sales_pipeline.schema
Remove-Item Env:BUNDLE_VAR_schema
databricks bundle deploy -t dev -p prowadzacy
databricks bundle run -t dev -p prowadzacy sales_job
databricks bundle run -t dev -p prowadzacy sales_job --params run_date=2026-11-02   # inny dzień tylko w tym runie

# 5.3: ten sam bundle na kolejnych targetach
databricks bundle deploy -t test -p prowadzacy
# klaster jobu w prod: wartość zmiennej complex z targetu (num_workers 1, bez profilu singleNode)
(databricks bundle validate -t prod -p prowadzacy -o json | Out-String | ConvertFrom-Json).resources.jobs.sales_job.job_clusters[0].new_cluster
databricks bundle deploy -t prod -p prowadzacy      # tylko z gałęzi main
databricks bundle summary -t prod -p prowadzacy     # zasoby targetu z linkami do UI
databricks bundle run -t prod -p prowadzacy sales_job

# 5.4: target domyślny (dev), bez -t
databricks bundle deploy -p prowadzacy
```

## Co pokazać w UI po runie
- Jobs & Pipelines: job `[dev <login>] sales_job_dev`, w runie graf trzech zadań.
- Pipeline `[dev <login>] sales_pipeline_dev`: graf `orders_clean` → `daily_sales` i `category_sales`, a przy
  `orders_clean` metryki jakości: 3 wiersze odrzucone, 1 wiersz przepuszczony z naruszeniem `known_region`.
- Catalog: `szkolenie.demo_dev` z tabelami `raw_orders`, `orders_clean`, `daily_sales` i `category_sales`.
- Wynik zadania `check_quality`: liczby wierszy partii i tabela z `daily_sales`.

## Po pokazie
W prod harmonogram działa, więc job uruchamiałby się codziennie o 6:00. Usuń wszystkie targety:

```powershell
databricks bundle destroy -t dev -p prowadzacy --auto-approve
databricks bundle destroy -t test -p prowadzacy --auto-approve
databricks bundle destroy -t prod -p prowadzacy --auto-approve
```

Destroy usuwa job i pipeline, ale nie tabele. `raw_orders` nie jest zasobem bundle'a, a tabele pipeline'u Unity
Catalog zostają po usunięciu pipeline'u i można je tylko czytać, dopóki nie trafią do działającego pipeline'u.
Przed kolejnym pokazem usuń je w edytorze SQL (grupa szkolenie-admini ma MANAGE na katalogu `szkolenie`):

```sql
DROP TABLE IF EXISTS szkolenie.demo_dev.raw_orders;
DROP TABLE IF EXISTS szkolenie.demo_dev.orders_clean;
DROP MATERIALIZED VIEW IF EXISTS szkolenie.demo_dev.daily_sales;
DROP MATERIALIZED VIEW IF EXISTS szkolenie.demo_dev.category_sales;
DROP TABLE IF EXISTS szkolenie.demo_test.raw_orders;
DROP TABLE IF EXISTS szkolenie.demo_test.orders_clean;
DROP MATERIALIZED VIEW IF EXISTS szkolenie.demo_test.daily_sales;
DROP MATERIALIZED VIEW IF EXISTS szkolenie.demo_test.category_sales;
DROP TABLE IF EXISTS szkolenie.demo_prod.raw_orders;
DROP TABLE IF EXISTS szkolenie.demo_prod.orders_clean;
DROP MATERIALIZED VIEW IF EXISTS szkolenie.demo_prod.daily_sales;
DROP MATERIALIZED VIEW IF EXISTS szkolenie.demo_prod.category_sales;
```

## Gdy coś nie działa
- Błąd przy `etl_sp` albo `run_as`: lookup szuka service principala po nazwie `szkolenie-etl`. Zapas: podaj jego
  application ID w poleceniu, `--var="etl_sp=<application ID>"` (Settings → Identity and access → Service
  principals). Nie wpisuj go do plików.
- Klaster jobu nie startuje: w dev i test potrzebuje 4 vCPU, w prod 8 vCPU, ponad 16 vCPU klastra
  szkolenie-interaktywny. Sprawdź limit vCPU rodziny DDSv5 w North Europe.
- `check_quality` kończy się błędem: komunikat w wyniku zadania mówi, który warunek nie przeszedł.
- Odświeżenie pipeline'u zgłasza, że tabela już istnieje: zostały tabele z poprzedniego pokazu. Usuń je jak
  w części „Po pokazie”.

## CI/CD
W repo nie ma pliku `.github/workflows/...`, bo GitHub uruchamiałby go przy każdym pushu. W szkoleniu CI/CD to tylko
fakt (lekcja 5.4), a wdrożenia idą z terminala.
