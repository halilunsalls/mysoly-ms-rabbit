# mysoly-ms-rabbit

**mysoly** akış mikroservisinin (`ms-mysoly-flow`) RabbitMQ API’sini tarayıcıdan test etmek için kullanılan bir **RabbitMQ istemcisi**. HTTP yerine kuyruk üzerinden giden her çağrıyı (pipeline, chat, log) aynı mesaj sözleşmesiyle gönderir, cevabı bekler ve gösterir.

Bu repo mikroservisin kendisi değildir. Mikroservis kuyrukları dinler; bu uygulama kuyruklara mesaj yazar.

```
┌─────────────────────┐         AMQP          ┌──────────────────────────┐
│  mysoly-ms-rabbit   │  ──────────────────►  │  RabbitMQ                │
│  FastAPI + Web UI   │  ◄──────────────────  │  kuyruk: pipeline / chat │
│  :8005              │   RPC reply queue     │        / logs            │
└─────────────────────┘                       └────────────┬─────────────┘
                                                           │ consume
                                                           ▼
                                              ┌──────────────────────────┐
                                              │  ms-mysoly-flow          │
                                              │  rabbit/router.py        │
                                              └──────────────────────────┘
```

---

## Ne işe yarar?

- Mikroservisin HTTP endpoint’lerinin RabbitMQ karşılığını (`pipeline.run` ≡ `POST /run`) tek ekrandan denemek
- Ortam bazlı kuyruk önekleriyle (`local_chat`, `prod_pipeline` …) aynı UI’dan farklı ortamları hedeflemek
- RPC (cevap bekle) veya fire-and-forget (sadece yayınla) göndermek
- Chat oturumunda dönen `session_id` gibi değerleri yakalayıp sonraki isteğe otomatik basmak
- `pipeline.diagram` / `chat.diagram` cevaplarındaki Mermaid diyagramını görüntülemek, SVG/PNG indirmek

---

## Gereksinimler

| Bileşen | Not |
|---|---|
| **Python 3.11+** | Proje `cpython-311` ile çalışıyor; 3.11 veya 3.12 önerilir |
| **pip** | `requirements.txt` kurulumu için |
| **RabbitMQ 3.x+** | AMQP `5672` açık olmalı |
| **ms-mysoly-flow** | Kuyrukları tüketen mikroservis ayakta olmalı; aksi halde RPC 504 timeout verir |

İşletim sistemi: Windows, macOS, Linux. Aşağıdaki komutlar PowerShell ve bash için ayrı yazılmıştır.

---

## Kurulum ve çalıştırma (build)

Derleme adımı yok: saf Python. Sanal ortam + bağımlılık + `uvicorn` yeterli.

### 1. Repoyu alın

```bash
git clone <repo-url>
cd mysoly-ms-rabbit
```

### 2. Sanal ortam

**Windows (PowerShell)**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**macOS / Linux**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Bağımlılıklar

```bash
pip install -r requirements.txt
```

Kurulan paketler:

| Paket | Rol |
|---|---|
| `fastapi` | HTTP API ve UI sunucusu |
| `uvicorn[standard]` | ASGI sunucu (`reload` dahil) |
| `aio-pika` | Asenkron AMQP istemcisi |
| `python-dotenv` | `.env` yükleme |
| `jinja2` | `templates/index.html` |
| `python-multipart` | Form / body parse |
| `httpx` | HTTP yardımcıları |

### 4. Ortam dosyası

Kök dizinde `.env` oluşturun (dosya git’e girmez). Hiçbiri zorunlu değildir; varsayılanlar `localhost` / `guest` / `5672`.

```env
# RabbitMQ bağlantısı
RABBIT_HOST=localhost
RABBIT_PORT=5672
RABBIT_USER=guest
RABBIT_PASS=guest
RABBIT_VHOST=/
RABBIT_TIMEOUT=30

# Kuyruk öneki: boş bırakılırsa kuyruk adı "chat"
# "local" → "local_chat", "prod" → "prod_pipeline"
RABBIT_ENV_PREFIX=

# Eski isimler de okunur (geriye uyumluluk)
# RABBITMQ_HOST=localhost
# RABBITMQ_PORT=5672
# RABBITMQ_USER=guest
# RABBITMQ_PASSWORD=guest
# RABBITMQ_VHOST=/
```

Mikroservis tarafında kuyruk adları bu önekle **aynı** olmalıdır. Örneğin UI’da **local** seçiliyse mikroserviste şuna benzer bir tanım beklenir:

```env
RABBIT_QUEUES=local_pipeline,local_chat,local_logs
RABBIT_ENV_PREFIX=local
```

Önek yoksa kuyruklar `pipeline`, `chat`, `logs` olur.

### 5. Uygulamayı başlatın

Proje kökünden (`main.py` ve `templates/` burada olmalı):

```bash
python main.py
```

veya:

```bash
uvicorn main:app --host 0.0.0.0 --port 8005 --reload
```

Uygulama **http://localhost:8005** adresinde açılır.

| Adres | Ne |
|---|---|
| http://localhost:8005/ | Web UI |
| http://localhost:8005/docs | Swagger (FastAPI otomatik) |
| http://localhost:8005/redoc | ReDoc |
| http://localhost:8005/api/ping | Broker bağlantı kontrolü |

`python main.py` zaten `0.0.0.0:8005` ve `reload=True` ile `uvicorn` başlatır.

### 6. Çalıştığını doğrulayın

1. RabbitMQ’yu başlatın (yerelde varsayılan guest/guest).
2. UI üst çubuğunda **Connect** — yeşil nokta broker’a ulaşıldığını gösterir (`GET /api/ping`).
3. Mikroservisin ilgili kuyrukları dinlediğinden emin olun.
4. Soldan `pipeline.list` seçip **Send** — cevap panelinde pipeline listesi gelmeli.

Cevap 30 sn içinde gelmezse `504` ve şu mesaj benzeri bir hata döner: kuyruk adı mikroserviste `RABBIT_QUEUES` ile tanımlı mı?

---

## Web UI

Üst çubuk: Host / port / kullanıcı / şifre / vhost → **Connect**. Değerler `POST /api/config` ile süreç içinde güncellenir (yeniden başlatınca `.env`’e döner).

**Env** butonları kuyruk önekini değiştirir:

| Seçim | Örnek kuyruk |
|---|---|
| — none | `chat` |
| local | `local_chat` |
| test | `test_chat` |
| rel | `rel_chat` |
| prod | `prod_chat` |

Sol menü üç grupta bilinen rotaları listeler. Bir rota seçilince payload şablonu dolar. **Wait for reply** açıkken RPC (geçici exclusive reply kuyruğu + `correlation_id`); kapalıyken sadece yayın.

**Captured** çubuğu cevaptan `session_id`, `chat_name`, `account_id`, `group_id`, `prod_name`, `pipeline` gibi alanları tutar ve sonraki şablona basar. `chat.run` sonrası `session_id` ile devam etmek için kullanılır.

**Builder**, JSON yerine form alanlarıyla (özellikle chat) payload üretir. **Mermaid** butonu / cevap içindeki diyagram, zoom ve SVG/PNG dışa aktarma sunar.

---

## HTTP API

UI aynı uçları kullanır; harici araçlarla da çağrılabilir.

### `GET /api/config`

Aktif bağlantı ayarları + `env_prefix` (şifre düz metin döner; sadece güvenilir ağda kullanın).

### `POST /api/config`

```json
{
  "host": "rabbit.internal",
  "port": 5672,
  "user": "app",
  "password": "secret",
  "vhost": "/",
  "timeout": 30
}
```

Boş bırakılan alanlar değişmez.

### `GET /api/routes`

`KNOWN_ROUTES` sözlüğü: kuyruk, pattern, HTTP karşılığı, örnek `data` şablonu, `capture` anahtarları.

### `POST /api/send`

```json
{
  "name": "chat.run",
  "data": {
    "chat_name": "my_chat",
    "prod_name": "default",
    "account_id": "account-1",
    "group_id": "group-1",
    "message": "Merhaba",
    "session_id": null
  },
  "rpc": true,
  "timeout": 30,
  "env_prefix": "local"
}
```

| Alan | Anlam |
|---|---|
| `name` | `kuyruk.pattern` (`chat.run`). Bilinen listede yoksa ilk noktadan bölünür |
| `data` | Mikroservise giden `data` gövdesi |
| `rpc` | `true` = cevap bekle; `false` = sadece yayınla |
| `timeout` | RPC saniye cinsinden bekleme (varsayılan 30) |
| `env_prefix` | Kuyruk öneki; boşsa `RABBIT_ENV_PREFIX` veya öneksiz ad |

Broker’a giden zarf:

```json
{
  "pattern": "run",
  "data": { "...": "..." }
}
```

Routing key: `{env_prefix}_{queue}` veya `{queue}`.

RPC başarılı cevap (özet):

```json
{
  "success": true,
  "name": "chat.run",
  "queue": "local_chat",
  "pattern": "run",
  "correlation_id": "...",
  "envelope_sent": { "pattern": "run", "data": {} },
  "reply_raw": { "data": { "payload": {} } },
  "reply": {}
}
```

`reply`, mikroservisin `{ "data": { "payload": ... } }` zarfından çıkarılmış iş yüküdür. Hata zarfı `{ "data": { "error": { "status", "message", "code" } } }` ise HTTP status o `status` değeriyle yükseltilir.

Fire-and-forget (`rpc: false`) `bytes` ve `envelope_sent` döner; cevap beklemez.

### `GET /api/ping`

Broker’a kısa bağlantı. Başarılı: `{ "status": "ok", "dsn": "amqp://user:***@host:port/" }`. Bağlanamazsa `503`.

---

## Mesaj sözleşmesi

Node.js / mikroservis ile uyumlu zarf:

| Yön | Şekil |
|---|---|
| İstek | `{ "pattern": "run", "data": { ... } }` |
| Başarılı cevap | `{ "data": { "payload": { ... } } }` |
| Hata cevap | `{ "data": { "error": { "message", "code", "status" } } }` |

Rota anahtarı: `queue + "." + pattern` → `chat.run`, `logs.pipeline`.

RPC’de istemci exclusive bir reply kuyruğu açar, mesaja `reply_to` ve `correlation_id` koyar. Mikroservis (`app_backend/rabbit/rabbit.py` içindeki `receive_message`) `reply_to` varsa sonucu bu kuyruğa yazar.

---

## Bilinen rotalar

`main.py` içindeki `KNOWN_ROUTES`, mikroservis `rabbit/router.py` `_ROUTES` tablosuyla aynı anahtarları kullanmalıdır.

### Pipeline

| Rota | HTTP karşılığı | `data` (özet) |
|---|---|---|
| `pipeline.run` | `POST /run` | `pipeline`, `input`, `dry_run`, `prod_name` |
| `pipeline.list` | `GET /pipelines` | `{}` |
| `pipeline.diagram` | `GET /pipelines/{name}/diagram` | `pipeline`, `format` (`json` / `text`) |

### Chat

| Rota | HTTP karşılığı | `data` (özet) |
|---|---|---|
| `chat.list` | `GET /chats` | `{}` |
| `chat.get` | `GET /chats/{name}` | `chat_name` |
| `chat.diagram` | `GET /chats/{name}/diagram` | `chat_name` |
| `chat.run` | `POST /chat/run` | `chat_name`, `prod_name`, `account_id`, `group_id`, `message`, isteğe bağlı `session_id` |
| `chat.session.list` | `GET /chats/{name}/sessions` | `chat_name`, `status`, `prod_name`, `group_id`, `account_id`, `limit`, `offset` |
| `chat.session.get` | `GET /chats/{name}/sessions/{id}` | `session_id` |
| `chat.session.messages` | `GET /chats/{name}/sessions/{id}/messages` | `chat_name`, `session_id`, `limit`, `offset` |
| `chat.session.end` | `DELETE /chats/{name}/sessions/{id}` | `chat_name`, `session_id` |
| `chat.session.vars` | `PATCH /chats/{name}/sessions/{id}/vars` | `chat_name`, `session_id`, `vars` |

`chat.run`: `session_id` yoksa / `null` ise mikroservis yeni oturum açıp mesajı işler; varsa mevcut oturuma devam eder.

### Logs

| Rota | HTTP karşılığı | Kaynak tablo |
|---|---|---|
| `logs.ai` | `GET /logs/ai` | `log_llm_call` |
| `logs.pipeline` | `GET /logs/pipeline` | `log_pipeline_run` |
| `logs.chat` | `GET /logs/chat` | `chat_session` |
| `logs.errors` | `GET /logs/errors` | `log_error` |

Filtreler isteğe bağlıdır (`limit` varsayılan 50, `offset` 0).

---

## Örnek çağrılar

### curl — pipeline listesi (RPC)

```bash
curl -s -X POST http://localhost:8005/api/send \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"pipeline.list\",\"data\":{},\"rpc\":true,\"timeout\":30,\"env_prefix\":\"local\"}"
```

### curl — chat turu

```bash
curl -s -X POST http://localhost:8005/api/send \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"chat.run\",\"rpc\":true,\"timeout\":60,\"env_prefix\":\"local\",\"data\":{\"chat_name\":\"my_chat\",\"prod_name\":\"default\",\"account_id\":\"a1\",\"group_id\":\"g1\",\"message\":\"Merhaba\"}}"
```

### Broker ping

```bash
curl -s http://localhost:8005/api/ping
```

---

## Proje yapısı

```
mysoly-ms-rabbit/
├── main.py                 # FastAPI uygulaması: UI + /api/* + AMQP istemcisi
├── requirements.txt
├── templates/
│   └── index.html          # Tek sayfalık istemci UI
├── favicon/
│   └── api.ico
├── app_backend/            # Referans: mikroservis Rabbit / HTTP katmanı (bu repo tek başına çalıştırmaz)
│   ├── api/
│   │   ├── main.py         # HTTP: /run, /pipelines, /logs/*
│   │   ├── chats.py        # HTTP: /chat/run, /chats/*
│   │   └── rabbit_proxy.py # Mikroservis içi HTTP→AMQP proxy
│   └── rabbit/
│       ├── rabbit.py       # aio-pika: listen, send, RPC
│       └── router.py       # pattern → handler (_ROUTES)
├── .gitignore              # .venv, .env, __pycache__
└── README.md
```

`app_backend/` buradaki `KNOWN_ROUTES` ile mikroservis router’ını hizalı tutmak içindir. `db`, `engine` ve pipeline tanımları bu repoda yoktur; gerçek motor `ms-mysoly-flow` içindedir.

Yeni bir rota eklenecekse:

1. Mikroserviste `router.py` `_ROUTES` + handler
2. Bu repoda `main.py` `KNOWN_ROUTES` (queue, pattern, desc, http, template, capture)
3. Gerekirse UI builder alanları (`templates/index.html`)

---

## Ortam değişkenleri (özet)

| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `RABBIT_HOST` | `localhost` | Broker host (`RABBITMQ_HOST` yedek) |
| `RABBIT_PORT` | `5672` | AMQP port (`RABBITMQ_PORT` yedek) |
| `RABBIT_USER` | `guest` | Kullanıcı (`RABBITMQ_USER` yedek) |
| `RABBIT_PASS` | `guest` | Şifre (`RABBITMQ_PASSWORD` yedek) |
| `RABBIT_VHOST` | `/` | Virtual host (`RABBITMQ_VHOST` yedek) |
| `RABBIT_TIMEOUT` | `30` | UI’daki varsayılan RPC timeout (sn) |
| `RABBIT_ENV_PREFIX` | *(boş)* | Varsayılan kuyruk öneki; UI Env butonları isteği override eder |

---

## Sık karşılaşılan sorunlar

**503 — RabbitMQ connection failed**  
Broker kapalı, host/port/vhost yanlış veya ağ erişimi yok. Management UI (`15672`) veya `GET /api/ping` ile kontrol edin.

**504 — No reply from '…'**  
Mesaj kuyruğa gitti; kimse cevap yazmadı. Mikroservis ayakta mı, `RABBIT_QUEUES` tam kuyruk adını (önek dahil) içeriyor mu, pattern handler’ı var mı?

**422 / `VALIDATION_ERROR`**  
Mikroservis zorunlu alanı reddetti (`chat_name`, `pipeline`, `message` …). Şablonu ve Captured değerlerini kontrol edin.

**404 — No handler / not found**  
`name` `_ROUTES` ile eşleşmiyor veya pipeline/chat/session yok.

**UI açılıyor, Connect kırmızı**  
Uygulama RabbitMQ’suz da ayağa kalkar. Connect / ping broker’a ayrı bağlanır.

**Port 8005 dolu**

```bash
uvicorn main:app --host 0.0.0.0 --port 8010 --reload
```

**Çalışma dizini**  
`templates/` ve `favicon/` `main.py` ile aynı kökten çözülür. `uvicorn`’u başka klasörden başlatmayın; veya `--app-dir` ile kökü verin.

---

## Geliştirme notları

- UI şablonu `Jinja2` ile `KNOWN_ROUTES` ve anlık config’i basar; rota eklemek için çoğu zaman yalnızca `main.py` yeter.
- AMQP bağlantısı her `/api/send` ve `/api/ping` çağrısında açılıp kapanır (kalıcı kanal yok).
- Mesajlar `DeliveryMode.PERSISTENT`; kuyruk declare burada yapılmaz — kuyruğu mikroservis (veya operasyon) oluşturur.
- `.env` ve sanal ortam commit edilmez.

---

## Lisans

Dahili mysoly aracı. Dağıtım ve lisans ekibinizdeki repoya göre geçerlidir.
