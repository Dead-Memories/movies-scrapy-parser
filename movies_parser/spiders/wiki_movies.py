import re
import scrapy
from movies_parser.items import MovieItem


class WikiMoviesSpider(scrapy.Spider):
    name = "wiki_movies"
    allowed_domains = ["ru.wikipedia.org", "www.imdb.com", "imdb.com"]

    start_urls = [
        "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83",
        "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B3%D0%BE%D0%B4%D0%B0%D0%BC",
    ]

    def __init__(self, start_urls=None, max_movies=200, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if start_urls:
            self.start_urls = [u.strip() for u in start_urls.split(",") if u.strip()]

        self.max_movies = int(max_movies)
        self._emitted = 0  # кол-во отданных фильмов

    # хелперы
    def _clean(self, text: str | None) -> str | None:
        if not text:
            return None
        text = re.sub(r"\[\d+]", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text or None

    def _first_year(self, text: str | None) -> str | None:
        if not text:
            return None
        m = re.search(r"\b(18|19|20)\d{2}\b", text)
        return m.group(0) if m else None

    def _infobox_value(self, response, header_variants: list[str]) -> str | None:
        """
        Достаём значение из инфобокса по заголовку строки.
        Работает для: Жанр / Режиссёр / Страна / Год и т.п.
        """
        for h in header_variants:
            td = response.xpath(
                '//table[contains(@class,"infobox") or contains(@class,"infobox_v2")]'
                f'//tr[th[contains(normalize-space(string(.)), "{h}")]]/td'
            )
            if td:
                parts = td.xpath('.//a//text()').getall()
                if not parts:
                    parts = td.xpath('.//text()').getall()

                parts = [self._clean(p) for p in parts]
                parts = [p for p in parts if p]

                if parts:
                    # убираемм дубли
                    uniq = []
                    for p in parts:
                        if p not in uniq:
                            uniq.append(p)
                    return ", ".join(uniq)
        return None

    def _extract_imdb_id(self, response) -> str | None:
        href = response.xpath('//a[contains(@href,"imdb.com/title/tt")]/@href').get()
        if not href:
            return None
        m = re.search(r"(tt\d+)", href)
        return m.group(1) if m else None

    # Парсинг категорий. Переход на подкатегории, ссылки и след.страницы
    def parse(self, response):

        # 1) Подкатегории 
        subcats = response.css("#mw-subcategories a::attr(href)").getall()
        for href in subcats:
            if href and href.startswith("/wiki/Категория:"):
                yield response.follow(href, callback=self.parse)

        # 2) Статьи в категории
        pages = response.css("#mw-pages a::attr(href)").getall()
        for href in pages:
            if self._emitted >= self.max_movies:
                return

            if not href or not href.startswith("/wiki/"):
                continue
            # убираем служебные пространства
            if ":" in href:
                continue
            # убираем ссылки на саму категорию
            if href.startswith("/wiki/Категория:"):
                continue

            # фильтр на редирект
            if "#" in href:
                continue
            yield response.follow(href, callback=self.parse_movie)

        # 3) Следующая страница категории
        next_page = response.xpath('//a[contains(text(),"Следующая страница")]/@href').get()
        if next_page and self._emitted < self.max_movies:
            yield response.follow(next_page, callback=self.parse)

# Парсинг страниц фильма
    def parse_movie(self, response):
        if self._emitted >= self.max_movies:
            return

        item = MovieItem()
        # item["title"] = self._clean(response.css("#firstHeading::text").get())
        title = response.css("#firstHeading::text").get()
        if not title:
            title = response.xpath('//h1[@id="firstHeading"]/span/text()').get()
        if not title:
            title = response.css("title::text").get() 
            if title:
                title = title.split("—")[0].strip()

        item["title"] = self._clean(title)

        item["genre"] = self._infobox_value(response, ["Жанр", "Жанры"])
        item["director"] = self._infobox_value(response, ["Режиссёр", "Режиссер", "Режиссёр-постановщик"])
        item["country"] = self._infobox_value(response, ["Страна", "Страны"])
        year_raw = self._infobox_value(response, ["Год", "Дата выхода", "Премьера"])
        item["year"] = self._first_year(year_raw)

        imdb_id = self._extract_imdb_id(response)

        # Если imdb_id есть — переход за рейтингом
        if imdb_id:
            imdb_url = f"https://www.imdb.com/title/{imdb_id}/"
            yield scrapy.Request(
                imdb_url,
                callback=self.parse_imdb,
                meta={"item": item},
                headers={"Accept-Language": "en-US,en;q=0.9"},
            )
        else:
            item["imdb_rating"] = None
            self._emitted += 1
            yield item

# Парсинг рейтинга (доп.задание)
    def parse_imdb(self, response):
        item = response.meta["item"]

        rating = None

        # 1) JSON-LD
        json_ld = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if json_ld:
            m = re.search(r'"ratingValue"\s*:\s*"?(?P<r>\d+(\.\d+)?)"?', json_ld)
            if m:
                rating = m.group("r")

        # 2) fallback по data-testid
        if not rating:
            rating = response.xpath(
                '//span[@data-testid="hero-rating-bar__aggregate-rating__score"]/span[1]/text()'
            ).get()

        item["imdb_rating"] = self._clean(rating)
        self._emitted += 1
        yield item
