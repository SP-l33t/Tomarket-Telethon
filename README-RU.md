[![Static Badge](https://img.shields.io/badge/Telegram-Channel-Link?style=for-the-badge&logo=Telegram&logoColor=white&logoSize=auto&color=blue)](https://t.me/+jJhUfsfFCn4zZDk0)      [![Static Badge](https://img.shields.io/badge/Telegram-Bot%20Link-Link?style=for-the-badge&logo=Telegram&logoColor=white&logoSize=auto&color=blue)](https://t.me/Tomarket_ai_bot/app?startapp=0000GbQY)

## Рекомендация перед использованием

# 🔥🔥 Используйте PYTHON версии 3.10 🔥🔥

> 🇪🇳 README in english available [here](README)

## Функционал  
|               Функционал               | Поддерживается |
|:--------------------------------------:|:--------------:|
|            Многопоточность             |       ✅        |
|        Привязка прокси к сессии        |       ✅        |
|   Авто Реферальство ваших аккаунтов    |       ✅        |
|    Автоматическое выполнение задач     |       ✅        |
|      Поддержка pyrogram .session       |       ✅        |
|              Авто фарминг              |       ✅        |
|   Автоматическое выполнение квестов    |       ✅        |
|        Авто Ежедневная Награда         |       ✅        |
|          Авто Получение Звезд          |       ✅        |
|          Авто Получение Комбо          |       ✅        |
|          Авто Прокачка уровня          |       ✅        |
| Поддержка telethon И pyrogram .session |       ✅        |

_Скрипт осуществляет поиск файлов сессий в следующих папках:_
* /sessions
* /sessions/pyrogram
* /session/telethon

## [Настройки](https://github.com/SP-l33t/Tomarket-Telethon-Telethon/blob/main/.env-example/)
|          Настройка          |                                                                                                                              Описание                                                                                                                               |
|:---------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|         **API_ID**          |                                                                                                                  Ваш Telegram API ID (целое число)                                                                                                                  |
|        **API_HASH**         |                                                                                                                   Ваш Telegram API Hash (строка)                                                                                                                    |
|   **GLOBAL_CONFIG_PATH**    | Определяет глобальный путь для accounts_config, proxies, sessions. <br/>Укажите абсолютный путь или используйте переменную окружения (по умолчанию - переменная окружения: **TG_FARM**)<br/> Если переменной окружения не существует, использует директорию скрипта |
|        **FIX_CERT**         |                                                                                              Попытаться исправить ошибку SSLCertVerificationError ( True / **False** )                                                                                              |
|         **REF_ID**          |                                                                                                                 Ваш реферальный ID после startapp=                                                                                                                  |
|      **POINTS_COUNT**       |                                                                                                          Количество поинтов за игру (например, [450, 600])                                                                                                          |         
|     **AUTO_PLAY_GAME**      |                                                                                                             Автоматически играть в игры (True / False)                                                                                                              |
|        **AUTO_TASK**        |                                                                                                           Автоматически выполнять задания (True / False)                                                                                                            |
|    **AUTO_DAILY_REWARD**    |                                                                                                      Автоматически получать ежедневные награды (True / False)                                                                                                       |
|    **AUTO_CLAIM_STARS**     |                                                                                                       Автоматически получать звездные награды (True / False)                                                                                                        |
|    **AUTO_CLAIM_COMBO**     |                                                                                                         Автоматически получать комбо-награды (True / False)                                                                                                         |
|    **AUTO_RANK_UPGRADE**    |                                                                                                 Автоматически прокачивать уровень при наличии звезд (True / False)                                                                                                  |
|   **SESSION_START_DELAY**   |                                                                                           Случайная задержка при запуске. От 1 до указанного значения (например, **360**)                                                                                           |
|   **SESSIONS_PER_PROXY**    |                                                                                           Количество сессий, которые могут использовать один прокси (По умолчанию **1** )                                                                                           |
|   **USE_PROXY_FROM_FILE**   |                                                                                              Использовать прокси из файла `bot/config/proxies.txt` (**True** / False)                                                                                               |
|  **DISABLE_PROXY_REPLACE**  |                                                                                   Отключить автоматическую проверку и замену нерабочих прокси перед стартом ( True / **False** )                                                                                    |
|      **DEVICE_PARAMS**      |                                                                                  Вводить параметры устройства, чтобы сделать сессию более похожую, на реальную  (True / **False**)                                                                                  |
|      **DEBUG_LOGGING**      |                                                                                               Включить логирование трейсбэков ошибок в папку /logs (True / **False**)                                                                                               |

## Быстрый старт 📚

Для быстрой установки и последующего запуска - запустите файл run.bat на Windows или run.sh на Линукс

## Предварительные условия
Прежде чем начать, убедитесь, что у вас установлено следующее:
- [Python](https://www.python.org/downloads/) **версии 3.10**

## Получение API ключей
1. Перейдите на сайт [my.telegram.org](https://my.telegram.org) и войдите в систему, используя свой номер телефона.
2. Выберите **"API development tools"** и заполните форму для регистрации нового приложения.
3. Запишите `API_ID` и `API_HASH` в файле `.env`, предоставленные после регистрации вашего приложения.

## Установка
Вы можете скачать [**Репозиторий**](https://github.com/SP-l33t/Tomarket-Telethon-Telethon) клонированием на вашу систему и установкой необходимых зависимостей:
```shell
git clone https://github.com/SP-l33t/Tomarket-Telethon-Telethon.git
cd Tomarket-Telethon
```

Затем для автоматической установки введите:

Windows:
```shell
run.bat
```

Linux:
```shell
run.sh
```

# Linux ручная установка
```shell
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
cp .env-example .env
nano .env  # Здесь вы обязательно должны указать ваши API_ID и API_HASH , остальное берется по умолчанию
python3 main.py
```

Также для быстрого запуска вы можете использовать аргументы, например:
```shell
~/Tomarket-Telethon >>> python3 main.py --action (1/2)
# Или
~/Tomarket-Telethon >>> python3 main.py -a (1/2)

# 1 - Запускает кликер
# 2 - Создает сессию
```


# Windows ручная установка
```shell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy .env-example .env
# Указываете ваши API_ID и API_HASH, остальное берется по умолчанию
python main.py
```

Также для быстрого запуска вы можете использовать аргументы, например:
```shell
~/Tomarket-Telethon >>> python main.py --action (1/2)
# Или
~/Tomarket-Telethon >>> python main.py -a (1/2)

# 1 - Запускает кликер
# 2 - Создает сессию
```

Код частично взят у @yanpaing007. Большое спасибо ♥