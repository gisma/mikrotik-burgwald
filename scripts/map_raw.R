# Pakete
req <- c("readxl","dplyr","stringr","sf","mapview","leafpop","htmlwidgets","leaflet")
to_install <- req[!sapply(req, requireNamespace, quietly = TRUE)]
if (length(to_install)) install.packages(to_install, repos="https://cloud.r-project.org")
lapply(c("readxl","dplyr","stringr","sf","mapview"), library, character.only = TRUE)

# 1) Excel laden
xls_path <- "assets/Burgwald_Koordinaten_ Sondentyp.xlsx"
df <- readxl::read_excel(xls_path)

# 2) Koordinaten säubern: Komma→Punkt, numerisch
if (!is.numeric(df$North)) df$North <- as.numeric(stringr::str_replace(as.character(df$North), ",", "."))
if (!is.numeric(df$East )) df$East  <- as.numeric(stringr::str_replace(as.character(df$East ), ",", "."))

# 3) (Optional) Vertauschte Spalten erkennen & fixen (falls mal East/West in North steckt)
swapped <- df$North >= 8 & df$North <= 9.5 & df$East >= 50 & df$East <= 51.5
df$North <- ifelse(swapped, df$East,  df$North)
df$East  <- ifelse(swapped, df$North, df$East)  # Achtung: wenn du das sauber willst, vorher in tmp speichern.

# 4) Zeilen ohne Koordinaten rausfiltern → df_ok
df_ok <- dplyr::filter(df, !is.na(North), !is.na(East))

# 5) „Sensoren“-Zusammenfassung bauen (✓ für vorhandene)
df_ok <- df_ok %>%
  mutate(across(c("Radar","Druck","Hobo"),
                ~ ifelse(is.na(.x) | .x=="", "", "✓"))) %>%
  mutate(Sensoren = trimws(paste0(
    ifelse(Radar=="✓","Distanz ",""),
    ifelse(Druck=="✓","Wassertiefe ",""),
    ifelse(Hobo =="✓","Div. Sensoren","")
  )))

# 6) sf-Punkte (WGS84)
pts <- sf::st_as_sf(df_ok, coords = c("East","North"), crs = 4326, remove = FALSE)

# 7) Popup-Spalten (defensiv wählen)
popup_cols <- intersect(c("Gewässer","Einzugsgebiet","Gemeinde","Sensoren","North","East"),
                        names(sf::st_drop_geometry(pts)))

# Karte: Legende nach „Sensoren“, Popup per Klick
m <- mapview::mapview(
  pts,
  zcol = "Sensoren",               # Legende/Farbe nach Sensortyp
  layer.name = "Geplante Messstellen",
  cex = 8,
  label = pts$Nr,                  # Hover-Label
  popup = leafpop::popupTable(
    sf::st_drop_geometry(pts)[, popup_cols, drop = FALSE],
    feature.id = FALSE
  ),
  legend = TRUE
)

# 9) Auf Extent zoomen (mit kleinem Rand)
bb  <- sf::st_bbox(pts)
pad <- 0.0001
m@map <- leaflet::fitBounds(
  m@map,
  lng1 = as.numeric(bb["xmin"])-pad,
  lat1 = as.numeric(bb["ymin"])-pad,
  lng2 = as.numeric(bb["xmax"])+pad,
  lat2 = as.numeric(bb["ymax"])+pad
)

# Anzeigen
m

# 10) Speichern (optional)
htmlwidgets::saveWidget(m@map, "assets/templates/karte_burgwald_sonden.html", selfcontained = TRUE)
