# 🚀 Instrukcja uruchomienia projektu

Poniższa instrukcja krok po kroku pokaże Ci, jak przygotować środowisko i uruchomić cały pipeline analizy danych.

---

## 🔧 Krok 1: Nadanie uprawnień do skryptów

Zacznij od nadania uprawnień wykonywalnych wszystkim skryptom `.sh` w bieżącym katalogu:

```bash
chmod u+x *.sh
```

> 💡 To polecenie sprawi, że wszystkie pliki z rozszerzeniem `.sh` będą mogły być uruchamiane bezpośrednio.

---

## 🛠️ Krok 2: Przygotowanie środowiska

Uruchom skrypt konfiguracyjny, który zainstaluje wymagane zależności i przygotuje środowisko:

```bash
./setup.sh
```

---

## 📓 Krok 3: Konwersja notebooków do skryptów Pythona

Jeśli modyfikujesz istniejące notebooki lub tworzysz nowe w katalogu `./notebooks`, skonwertuj je na pliki `.py` za pomocą:

```bash
./convert_notebooks.sh
```

> ✅ Skrypt ten automatycznie przekształci wszystkie notebooki z katalogu `./notebooks` i zapisze je jako pliki Pythona w katalogu `./scripts`.

---

## 📥 Krok 4: Pobranie danych

Uruchom skrypt, który pobierze wszystkie potrzebne dane i zapisze je w formacie **Parquet** w katalogu `./out`:

```bash
./download_data.sh
```

---

## 🧩 Krok 5: Przygotowanie ramki danych

Po zebraniu danych, połącz je w jedną spójną ramkę i dodaj własne cechy (ang. *custom features*):

```bash
./prepare_dataframe.sh
```

> 📌 Cechy niestandardowe są definiowane w pliku:  
> `./scripts/feature_engineering.py`

---

## 🤖 Krok 6: Uruchomienie modelu

Na koniec uruchom model testowy:

```bash
./test_model.sh
```

---

## 🔍 Dodatkowe: Walidacja prognoz

Jeśli chcesz zweryfikować jakość swoich prognoz lub cech, możesz skorzystać z dedykowanego skryptu walidacyjnego:

```bash
python ./scripts/validate_forecast.py pv_actual pv_forecast
```

> 📊 Skrypt ten pozwala na porównanie rzeczywistych wartości (`pv_actual`) z prognozowanymi (`pv_forecast`) i ocenić wartość prognoz.

---

Powodzenia! 🌟  
Jeśli napotkasz jakiekolwiek problemy — sprawdź logi lub skontaktuj się z zespołem ds. danych.

