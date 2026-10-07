# Implementation history before the honest-FC correction

Historical implementation notes moved from vault ADR 234 on 2026-10-07.
All FC timings and PASS claims below used TC=0 unless explicitly stated.
They are historical observations, not release qualification against default PGO.
The decision and current acceptance criteria remain in ADR 234.

## Квалификация и следующий этап 2026-10-07

История сохраняет ранние checkpoints. Linux runner: 2 физических / 4 logical CPU;
scaling учитывает physical cores; ×6 для ≥8 ядер и FC/allocation budgets не ослаблены.
Первый park: [[../../90-snippets/amalgam-parked-thread-warmup]].

## Inline L1-коммит и версии активной фабрики 2026-10-07

В видимом `main` реализуется выбранный при build план `InlineMemory`: builtin MemoryOnly
без backplane и distributed locker. Set выполняет синхронный L1-коммит без owned scope,
CancellationSource, Box::pin, tokio lane и receipt-канала. Подготовка
значения/метаданных — до storage guard, освобождение заменённого значения и observers —
после него. Возвращается завершённый receipt; запись работает без runtime. Custom memory
locker не подменяется при factory acquire.

Для версий рассмотрены отдельная lane-таблица и версия рядом с L1. Выбрана версия рядом
с записью: проверка снимка и изменение L1 происходят под одним storage guard, без
второго coordination lookup. Активные снимки держат Arc<Revision>, карта — только Weak.
После последнего снимка ключ удаляется: idle tombstones не копятся. Set/remove/expire
меняют активную версию, clear — поколение; насыщение terminal. Снимок взят до вызова
фабрики. Позднее вычисление возвращает свой результат инициатору, но не заменяет более
новую запись и не воскрешает awaited remove. Старое ожидание resurrection в regression
явно заменено: это улучшение контракта, а не заявление о совпадении с FC в этом случае.
Просроченный отвергнутый origin не инвалидирует более новый origin.

Промежуточные числа inline-коммита сохранены в истории этой заметки.

Проверен отдельный прототип стабильного pinned poll (не production integration): одна
аллокация async-task, !Unpin остаётся по одному адресу, wake не вызывает рекурсивный
poll, original panic лидеру / typed panic followers. Позднее enqueue после запроса
cancel требует дождаться фактического Drop future; пустая очередь сама по себе не
доказывает завершения отмены. См. [[../../90-snippets/amalgam-pinned-poll-
cancellation]].

## Single-flight: стабильное владение и готовый результат 2026-10-07

Таблица активных flight — 64 шарда; caller владеет подпиской, таблица — фабрикой. Ready
не регистрирует owned scope/task; Pending сохраняет наблюдаемую работу. Версия/cancel
встроены в flight; снимки возвращают idle version key даже при сохранённом фабричном
token. Один builtin key-lock пока координирует все пути. Стековый poll с переносом
отвергнут для произвольного !Unpin и RS-3. Выбрана одна Pin<Box> до первого poll:
отклонение от буквального «без Box до Pending» сохранено, новой unsafe-границы нет;
allocation-бюджет ≤6. Poll/Drop — вне внутренних guards. Лидер получает исходную panic,
ведомые — FactoryPanicked; Error::source сохраняет конкретный тип через SharedSource.
Закрытие не требует повторного caller poll; conditional refresh сохраняет stale snapshot
также при per-call FS off. Darwin std::Mutex allocations в cancellation и Tokio key-
lock/Notify заменены штатным parking_lot без обновления закреплённых зависимостей. Общий
key-lock сохранён до согласованной миграции native/eager/advanced; ранние результаты — в
истории.

## Reader slots: один writer gate и прямое владение 2026-10-07

Захват всех mutex-полос на каждую запись заменяется одним writer gate и постоянными
владельцами reader slots. Обычный читатель публикует счётчик только своей полосы;
совпавшие индексы используют отдельный счётчик в этой же полосе. SeqCst fence между
публикацией и проверкой gate симметричен закрытию gate и сканированию счётчиков
писателем. Bitmap фактически использованных полос публикуется **до owner CAS**: иначе
другой читатель мог бы видеть owner до того, как писатель узнал о полосе. Writer
проверяет только установленные биты, а не весь префикс до случайного process-wide
индекса.

Альтернатива — прежний захват каждого RawMutex в фиксированном порядке. Выбран gate:
устраняет число RMW, пропорциональное ядрам, и не превращает конкуренцию в промах.
Минимум 64 padded slots (либо next-power-of-two от большего available_parallelism); для
64 шардов это примерно 512 КиБ reader storage на кэш против 128 КиБ прежних 16 slots на
M4. Это явная цена избежания fallback после смены поколений потоков. Writer ждёт
короткие reader decisions через bounded spin/yield; долгий пользовательский Clone
увеличивает CPU ожидания. Reader parking использует Condvar, интерес и проверка gate
разделены fence, уведомление берёт mutex регистрации. Нулевые аллокации доказаны на 64
реально припаркованных прогретых читателях. Б ounded модели Loom проверяют first-use,
collisions и отсутствие lost wakeup; это дополнение, не формальное доказательство всех
исполнений. У Loom 0.7.2 SeqCst load/store ослаблены до AcqRel, поэтому протокол
использует поддерживаемые явные fences, без игнорирования alarms:
https://github.com/tokio-rs/loom#unsupported-features.

L1 mutation передаёт подготовленный Entry в карту без дополнительного Arc-clone;
отвергнутый кандидат остаётся в commit до освобождения внешней координации. Retirements
— закрытые None/One/Many: обычная замена не выделяет Vec для одного значения. Проверка
поймала потерю lifetime pin при уничтожении storage до outer guard; исправлено
сохранением pin **только** для deferred operation reclamation. Сам тест и его ожидания
сохранены. Проверка skip-memory-write origin теперь атомарна с advancement активной
версии и не инвалидирует более новую фабрику.

Промежуточная квалификация reader-gate сохранена в истории заметки.

## Подготовка и владение обычной записью 2026-10-07

Заимствованный storage key убрал выделение Arc строки для существующего ключа: 3 пары,
set 191.1 нс / FC 166.4, 1 аллокация; cold 1091.9 / 1468.1, 5.137 аллокации. Только
замену эксклюзивно принадлежащей карте записи можно выполнить через безопасный
Arc::get_mut без новой аллокации. Удержанный Entry, owned-cloner source или weak-
reference запрещает reuse, сохраняя снимок. Старый payload переносится наружу и
уничтожается после writer guard; исходные eviction values сохраняются. Альтернатива
выделять новую Arc для каждой замены остаётся fallback. Промежуточные числа reuse
сохранены в истории заметки. **Аллокационный выигрыш не закрывает бюджет времени set.**

CPU-профиль standalone set показал два чтения Instant и повторную подготовку временных
метаданных. Выбран единый private elapsed sample для timestamp и physical start;
supplied Clock сохраняет самостоятельный elapsed backend deadline. Неизменяемые default
lifetimes без jitter/eager готовятся при build, с полным wide/saturating диапазоном
Timestamp. Explicit options остаются валидируемым общим путём. Это реализация build-
selected pay-for-use; следующий замер определит её результат. Публикация
crates.io/GitHub release разрешена владельцем после завершения цели; Akzhol обновляется
отдельной задачей.

Checkpoint688: функциональные checks PASS, performance FAIL; числа сохранены в истории.
На запись теперь приходится один storage lookup; пустая origin-map не хеширует ключ.
Lazy mutation request не строит Hybrid future до настоящего Pending; default options
остаются заимствованными до выбора owned-плана. CI769: функциональные jobs PASS, scaling
FAIL; это не квалификация нового checkpoint.

## Проверенные checkpoints 2026-10-07

`f0939d5`: Pending/фон — TaskTracker + CancellationToken. Notification ставит cancel в
очередь; user Drop вне guards, tracker отпускается после фактического Drop. Set
IntoFuture → Result<()>, receipts opt-in, defaults overlay и raw tags. `4b45767`:
WeakSlots 64 шарда, локальная очистка и fair maintenance; без global lookup counter.
Совмещённый shutdown/storage fence замедлил ARM 69→83 нс, поэтому он выбирается только
x86, ARM plain сохранён. Bounded Loom и blocked-Clone PASS. `96dc381`: Cooperative и
retained L1 ordinary; strict выбирает Fenced/continuity/wait. L2-only без periodic
clear; expire Remove; soft требует stale-entry; recovery5 с. Try_build отвергает
отсутствующие owned lifetime/token и atomic value writes. 663 default + strict clippy
PASS, FC2.9 availability oracle 9 сценариев в repo PASS. `5d3f0ff`: native view
разделяет deferred admission source Scopes; 4 x86 Clone/close, strict x86 clippy, 26 ARM
sync/destructor contracts PASS.

`9217042`: lazy get_or_set отделяет input от pinned execution. Defaults overlay,
строковые tags, fallback/token, receipt opt-in. BlockingRuntime принимает IntoFuture;
ручные poll callers мигрированы без изменения assertions. 667 default с doctests, strict
all-target/all-feature clippy, fmt PASS. Plain pending !Unpin ownership, panic,
copy/cancel/Drop и native coordination сохранены. Фабрика пока Product API.
Промежуточные замеры этого checkpoint сохранены в истории заметки. Публикация разрешена
после финальных gates; Akzhol не обновляем в этой цели.

## General/hybrid ownership 2026-10-07

Выбрано удерживать только начатую фабрику + коммит, а не весь caller request: fallback,
lock timeout и observation принадлежат отдельным вызывающим. Новый retained-origin
использует тот же стабильный pinned flight, что plain; общий local lock координирует
plain/general/native/eager. Pending ждут через TaskTracker; без runtime следующий caller
помогает прежнему flight. Регистрация уже ожидающего до установки фабрики защищена
подпиской Notify перед повторной проверкой таблицы. Ready/default hit не создаёт эту
таблицу. CallerDropped/ScopeFinished не отменяют начатую фабрику; явная отмена остаётся
прямо связана с factory даже после дропа caller. Shutdown и lease loss сохранены. Паника
foreground сохраняет исходный payload для лидера, ожидающие получают FactoryPanicked;
background supervision сохраняет исходную join-причину. Семь новых публичных
RS-3/RS-4/race/cancel/panic контрактов + 14 regressions PASS. Прежний
cancelling_finite_factory fixture требовал отмены через caller Drop, что противоречило
RS-3. Он переведён на явный CancellationSource; итоговые assertions освобождения (0,0)
сохранены. Дроп отдельно проверяет удержание. Fenced backend loss по-прежнему
Lease(Lost), factory token — LeaseLost. 674 default, strict clippy, fmt и 4 targeted
live Valkey contracts PASS; container удалён. M4 all gate этого checkpoint FAIL по set;
числа сохранены в истории заметки. CI921: 14 функциональных jobs PASS; scaling FAIL,
числа сохранены в истории. API8, custom markers, полная FR/RS матрица и release
qualification ещё открыты.

## Подготовленные локальные deadlines 2026-10-07

Вместо проекции Instant→UTC на каждом plain hit выбран срок Instant при записи. Local
logical/physical deadlines — закрытый вариант, пересчитываются также при reuse и expire.
Один sample после reader admission проверяет обе границы. UTC metadata сохраняются для
ordering/адаптации; supplied clocks, Hybrid и публичный MemoryStore сохраняют прежние
UTC + elapsed правила. Слоты, Clone, cancellation/shutdown и детерминированный Drop не
ослабляются. 678 default, strict ARM/x86 clippy и x86 fence PASS; прошлые M4/Rosetta
замеры — в истории, Linux budgets открыты.

## Borrowed factory hits 2026-10-07

Native origin initializer отложен до miss/eager; default hit не клонирует Worker или
executor. Альтернатива — отдельная предварительная проверка L1 — отвергнута: одна
admission/observation переносится в продолжение без второго события. Персональная eager-
запись при plain defaults требует recheck под прежней admission; обнаруженная регрессия
исправлена без изменения старого sync assertion. 684 default на Rust1.95 и 18 x86
contracts/strict ARM+x86 clippy на MSRV1.88 PASS. Оба API hit0alloc. Driver входит в
identity, generated C# исключён; прошлые M4/Linux таблицы — в истории. API8/FR/release
открыты.

## Заимствованная версия cold-фабрики 2026-10-07

Выбрано заимствование storage/key у удержанного CacheInner вместо двух отдельных Arc-
владельцев. RevisionSnapshot сохраняет source и generation; Expected сравнивает снимок
атомарно с коммитом. Drop не снимает чужую более новую регистрацию. 685 default/doctests
и strict clippy MSRV1.88 PASS; прошлые M4 all PASS — в истории. Разница cold около 1% не
считалась выигрышем; Linux/get qualification оставались открыты.

## Изоляция сравнительных workloads 2026-10-07

Default-policy эксперимент снят: ускорения нет, production не меняется; история содержит
patch/reports. Set/cold — одинаковые fresh processes, 1M/100K в обоих runtimes; это
исправление измерения, не ускорение кэша.

## План копирования без callbacks 2026-10-07

Для standalone builtin L1 build выбирает отдельный план только для встроенных primitive
V и входов без Drop. Альтернатива по одному needs_drop<V> отвергнута: тип без Drop всё
равно может иметь пользовательский Clone. Новый публичный тест доказывает drainage
такого Clone в async/native; observers, eager, tokens, plugins, custom clocks и все
мутации остаются counted. Начальный close проверяется после reader admission до копии,
поздний — после неё. На miss фабрика counted. 686 MSRV default и strict ARM/x86 clippy,
x86 origin/eviction/fence PASS. M4 3 пары: read34/FC290, native28.6/253, get43.4/324,
native44.3/265 нс; distinct8 read4.65/53.39, get5.93/66.12, hit0alloc. Set≈126/139 all
FAIL; cold≈1050/2077 PASS. Linux CI теперь проверяет оба API, бюджеты прежние.

## Ненаблюдаемая замена и планы build 2026-10-07

Unique replacement выдаёт только V для Drop после guard; alias/capture/AtRetirement
сохраняют полный immutable snapshot. Copy plan валидируется в build, standard jitter без
Arc/dyn; configured callbacks вызываются даже при max=0. Arc56/inline144, first-reader
phase и unique Box прототипы сняты: нет выигрыша либо дополнительная
аллокация/неверифицированный weak-memory контракт.

Исправлены общая terminal Scope/token transition (первая причина сохраняется до
result/Drop) и Redis fault fixture DEL→RENAME: атомарная RENAME исключает стирание
concurrent replay. Прежние assertions сохранены; детерминированные cancellation
regressions и 50live повторов PASS. Подробности [[../../90-snippets/amalgam-redis-
journal-restore-race]].

Принят default L1 pipeline с V и 24 байтами lifetime facts: полный immutable envelope
строится для нового slot, alias/capture/deferred/retained; unique reuse сбрасывает все
старые facts, V уничтожается после guard. Public Metadata144 байта не меняется. Same-
machine3pairs set99.59/115.69, after-read99.69/115.42 нс≈14% лучше; eager/tagged
allocations0/0/3 прежние. Семь M4pairs/API: set90.26/FC146.37 и 90.57/146.23,
cold1175.08/2443.30 и 1169.72/2445.88; warm/set0alloc, cold5.141. ALL FAIL только 8core
scaling5.82/5.98<6; gate не ослаблен.

572e8e7 main: native Linux comparative ALL PASS обоих API (37592952593),
EPYC7763,2physical/4logical,7pairs/API. Read same8 43.416/FC159.623, distinct8
42.368/153.794, set242.340/362.241, cold2773.476/4443.427. Get same8 49.440/185.839,
distinct8 48.399/177.959, set240.990/363.942, cold2763.307/4221.096 нс/op.
Warm/set0alloc, cold5.12534. Это 8threads на 2physical cores, не 8core qualification и
не before/after с другим CPU.

e199ee4 исправил Clippy1.99 lint в нетаймируемом assertion. CI37593891035:14functional
PASS, comparative FAIL. EPYC9V45,2physical/4logical: read set170.743/FC214.746,
get173.076/216.793≈0.80× вместо≤0.75; native get≈0.503×, отличие около 1% не сигнал.
Cold PASS;8core qualification открыта.

d3e6b18 writer-admission candidate проверен native CI37598874592:15gates+aggregate PASS.
Однако обязательный same-runner before/after против e199 на EPYC7763,3counterbalanced
pairs показал set241.533→256.451 нс (+6.176%), plain256.034→270.748 (+5.747%), after-
read257.336→269.075 (+4.562%). Выбрано отклонить runtime/model, восстановление 0f9ab4e
на main;693ARM default/doctests и strict all-target/all-feature clippy PASS. Jitter
regression и reusable before/after driver сохранены. Сравнение против FC на другом CPU
не принимается за доказательство улучшения.

Найден пропущенный обязательный budget L2(in-memory)+JSON≤FC из target architecture.
Добавлен matched public-options fixture: SkipMemoryRead в обоих runtime, hydration
включена, L1=11/L2=7, factory запрещена, warmup/checksum проверены,300K операций
отдельно от warm/mutation процессов. --gate all теперь требует L2≤FC для обоих API. Семь
alternating pairs после отката на macOS arm64,12physical/12logical,
Rust1.88/.NET10.0.300/FC2.9: read ALL PASS, get FAIL только L2. Read
L2=1881.986/FC1904.350 нс (диапазоны пересекаются, считать≈паритетом),28alloc/op; get
L2=2311.358/2042.451 (+13.166%, непересекающиеся диапазоны),29.008alloc/op.
Set94.159/147.239 и 93.683/146.095; cold1216.635/2431.083 и 1197.082/2446.848 нс,
warm/set0alloc, cold5.141. Distinct scaling6.741/7.213≥6,8core qualification этого
working tree PASS обоих API; финальные API/source и Linux L2 gate ещё нужны. Baseline
diagnostic компилирует один текущий frozen harness против обеих версий, manifests и
фактические harness fingerprints должны совпасть.

Native CI37603067620 точного dd50 завершилась:14functional PASS, comparative и aggregate
FAIL. EPYC9V74,2physical/4logical,7pairs/API: read L2=3170.937/FC2670.806 нс
(+18.726%,24alloc); get L2=4020.895/3052.316 (+31.733%,25.008alloc), диапазоны не
пересекаются. Остальные warm/cold/write budgets PASS, set≈264–266/FC358,
cold≈2753–2764/4365–4370. Поэтому локальный 13% разрыв не универсален и релиз
заблокирован L2.

fa6e0b2 доставлен main: nested first-poll tracking и синхронная попытка свободного
builtin mutex; занятый ожидается обычным Tokio FIFO, cooperative scheduling глобально
включён. RED/GREEN512ready polls и queued-waiter regression сохранены. 698ARM
default/doctests,80x86 unit, strict MSRV all-target/all-feature ARM/x86 clippy PASS.
Локально 3pairs против dd50: L2read −5.12%, get −7.74%, cold −13.85%, set без выигрыша;
L2read26/get27alloc. Семь FCpairs/API на 12physical: read ALL PASS, get FAIL только L2
(+5.14%). Подробности [[../../90-snippets/amalgam-ready-mutex-cooperative-budget]].
Native37608572833 точного fa6 завершилась:14functional PASS, comparative/aggregate FAIL;
EPYC9V74,2physical/4logical,7pairs/API. L2read2405.747/FC2072.508 (+16.08%,22alloc),
get3058.638/2294.267 (+33.32%,23alloc). Остальные warm/cold/write budgets PASS.
Обязательный same-runner3pairs dd50→fa6: get3236.144→3091.064 (−4.48%),
cold2226.956→2166.386 (−2.72%); read2457.516→2493.483 (+1.46%, диапазоны пересекаются —
выигрыш не заявлен). Разные CI runs с одной моделью CPU не являются before/after.
Выбрано заимствовать вложенный L2 future/key/Worker у уже owned родительского Execution
вместо Arc-shared Worker (лишняя аллокация каждой операции). Четыре production callers
audited: read/get/eager/passive уже под Execution. Независимый phase Control объединяет
Request/checkpoint; pinned work и borrowed checkpoint без второго Box/Scope/TaskTracker.
Parent сохраняет counting/drainage через poll/Drop, включая shutdown без caller re-poll;
phase deadlines не отменяют origin, первая причина сохраняется. Четыре новых регрессии,
cooperative codec contracts,699ARM default/doctests,81x86 unit и strict MSRV all-
target/all-feature ARM/x86 clippy PASS. Panic probe должен оставаться полем pinned
future: произвольные user stack locals unwind до возврата executor и не являются cache-
controlled retirement; [[../../90-snippets/amalgam-shutdown-drop-reason-race]]. Локально
3counterbalanced frozen-harness pairs против fa6: L2read1686.317→1499.558
(−11.08%,26→23alloc), get2065.743→1816.523 (−12.06%,27→24alloc), set/cold около 1% не
сигнал. Дополнительные 7pairs с одинаковыми owner cache.rs edits: L2read −8.68%, get
−10.38%; warm1 medians≈+5%, distinct get больше,8worker ranges широкие и пересекаются —
неизменность warm timing не заявлена. Семь matched FCpairs/API на 12physical/12logical:
оба ALL PASS; readL2=1470.772/FC1753.618, get=1848.484/1946.456, set92.180/142.242,
cold935.510/2065.205, warm/set0alloc, cold5.009; scaling7.403/7.355≥6. 72f436c доставлен
main, source identity локальных замеров содержит owner cache.rs edits. Native37612542515
comparative завершена:
EPYC9V74,2physical/4logical,Rust1.88/.NET10.0.401(runtime10.0.12),7pairs/API.
ReadL2=2254.107/FC2110.394 (+6.81%,20alloc), get=2906.801/2334.241 (+24.53%,21alloc),
FAIL только L2; warm/cold/write budgets PASS. Exact-source same-runner3pairs fa6→72:
read2399.417→2255.874 (−5.98%), get3064.634→2905.989 (−5.18%), оба непересекаются
и−2alloc; set/cold около 1% не сигнал. Native get warm1/8 около 0.3%, same-read8 +3.13%
— неизменность всех warm timings не заявлена. Native37612542515 завершён: все
14functional milestone jobs PASS, comparative/aggregate FAIL по L2. Архитектурный шаг
оценивается по бюджету этапа согласно owner task, без микротюнинга±5%; L2 native budget,
Must и release qualification остаются открыты.

Локальное participation закрытое Local/Cooperative/Fenced; только actual lease
удерживает cleanup Tasks/Events/key. 7c01404 доставлен main, 699ARM default/81x86 unit и
strict MSRV lints PASS; local timing gain не заявлен. Native exact-source CI37617049677
завершён:14functional PASS, comparative/aggregate FAIL только L2. Xeon
Platinum8370C,2physical/4logical,Rust1.88/.NET10.0.401(runtime10.0.12),7pairs/API:
read3151.362/FC2569.427 (+22.65%,20alloc), get3758.472/2870.652 (+30.93%,21alloc);
остальные бюджеты PASS. Same-runner72→7c: read3223.351→3136.218 (−2.70%),
get3859.182→3784.180 (−1.94%, не считать сигналом). Разный CPU не доказывает регрессию
против прежней EPYC таблицы. Untimed allocation stacks выявили rebuild idle
coordination:3 лишних allocations/read и 4/get. Выбран ограниченный пул 16scalar
controls ×64shards, sealed только Mutex<()>/KeyLane без V; active holders/waiters/FIFO
не заменяются при eviction. Пул выбирается build только для Hybrid, MemoryOnly остаётся
Transient: безусловный пул дал cold+7.95% с непересекающимися диапазонами. Диагностика
вынесена в отдельный ignored bench, timed allocator исходный. Hybrid-only selection
доставлен main5a20b7814e69041f1f27ee7e40b160254bedcddb. 702ARM
default/doctests,84x86unit,strict all-target/all-feature clippy ARM/x86 на MSRV1.88,fmt
и 2ownership probes PASS. Три counterbalanced frozen-harness pairs против 7c:
L2read1511.814→1449.279 (−4.14%,23→20alloc), get1886.566→1775.102 (−5.91%,24→20), оба
диапазона не пересекаются. Cold967.295→987.970 (+2.14%,диапазоны
пересекаются),5.009alloc в обеих версиях; set+1.96% и отдельные warm medians не
считатьвыигрышем. Семь FCpairs/API на
macOS12physical/12logical,Rust1.88/.NET10.0.300(runtime10.0.8): оба ALL PASS,
L2read1445.370/1836.396, get1793.913/1981.606, set92.043/144.143, cold994.764/2269.843;
scaling7.576/7.436, warm/set0alloc. Local source identity сохраняет две owner inline-
edits. Exact-source CI37621686618 завершён:14functional PASS, comparative/aggregate FAIL
L2 и set. EPYC9V45,2physical/4logical,Rust1.88/.NET10.0.401(runtime10.0.12),7pairs/API:
L2read2099.981/FC1523.896 (+37.80%),get2496.249/1701.081 (+46.74%),17alloc оба;
set188.947/234.136 и 184.039/229.466≈0.80× вместо≤0.75; warm/cold PASS. Same-
runner7c→5a: get2563.028→2454.765 (−4.22%,диапазоны не пересекаются), read−4.70% с
пересечением;set+0.57%,cold−1.17% не сигнал. Разные CPU не before/after. Typed-observer
доставлен main9872a5d9a6cd2900b1e2a033056d1d7eac2f1c5c и убирает второй Box driver,
сохраняя pinned root Scope и lazy first-poll Explicit link всех 7reasons; caller token
удерживается до completion, failed-preparation adapter прежний. Общие Scope/Hybridqueue
этим не устранены. 705ARM/87x86,strict all-target/all-feature MSRV lints
ARM/x86,fmt,2probes PASS. 3pairs против 5a с одинаковыми owner edits:
L2read1438.652→1416.439 (−1.54%,не заявлять timing gain), get1782.023→1696.434
(−4.80%,непересекаются),20→19alloc оба; sync get39.197→41.429
(+5.69%,непересекаются),остальные бюджеты PASS,не заявлять неизменность всех warm
timings. 7FCpairs/API local ALL PASS,Rust1.88,12physical/12logical: L2read1405.042/1811.
911,get1690.867/1959.653;set91.437/142.059,cold969.857/2118.616;scaling7.583/7.559,warm/
set0alloc,cold5.009. Exact-source CI37626045385 завершён:14functional PASS,
comparative/aggregate FAIL только L2.
Xeon8370C,2physical/4logical,Rust1.88/.NET10.0.401(runtime10.0.12),7pairs/API:
read3048.267/FC2551.246 (+19.48%),get3597.918/2888.090 (+24.58%),16alloc оба;
warm/cold/write PASS. Same-runner5a→987: read+0.15%,get−1.31%,диапазоны пересекаются
и≤2%,native timing gain не заявлять;17→16alloc. Выбран следующий этап: cache-bound
первый poll под transferable striped admission, stable PinBox; Ready без
Scope/TaskTracker subscription, только Pending передаётся в Scope доосвобождения
admission. Global cache shutdown view действуетво время inline callbacks; owned
promotion передаёт terminal ordering настоящему Scope. Internal phases не создаютвторой
shutdown subscription; exported root/child tokens регистрируют cross-cache link.
Explicit caller и specialized sources пока owned. 8 новыхрегрессий,713ARM
default/doctest+95x86unit,strict all-target/all-feature MSRV
ARM/x86clippy,fmt,2ownershipprobes PASS. Этап доставлен
main928f1be9b2dfe1f4f3fa138c968a82339862b19a. Local3pairs против 987:
L2read1426.833→1318.168 (−7.62%),get1695.028→1647.283 (−2.82%),оба
непересекаются,19→14alloc; read distinct8+5.48% отмечен,не заявлять неизменность warm.
7FCpairs/API local ALL PASS:read1332.267/1793.057,get1691.025/1961.862,set89.720/142.738
,cold966.151/2134.264;scaling7.229/7.637,warm/set0alloc,cold5.009. Native CI37632808648
завершён:14functional PASS, comparative/aggregate FAIL только L2.
EPYC7763,2physical/4logical,Rust1.88/.NET10.0.401(runtime10.0.12):
read2780.475/FC2728.751 (+1.90%,пересечение/<2% не сигнал,но budget
FAIL),get3784.797/3041.129 (+24.45%),13alloc. Same-runner987→928: get+7.03%с
непересекающимися диапазонами — реальная регрессия,16→13alloc не доказываетускорение.
Следующий этапдоставлен main f277bf62a7518563c917e3a060fac3681c749a0b: свободная
mutation admission через scalar CAS, suspended claimants через key-local FIFO в 64shards
вместо per-keyTokio mutationmutex;
FIFOreservation/queuedcancellation/unpolledgrantrelease, fencedprotocol/reclamationorder
прежние. Shutdowntracking создаётся только для exportedlink; childclosingview сохранена,
отказотнеёотвергнут: child видит closing до queuedparentnotification.
8queue+2cancellationcontracts,723ARM default/doctest+105x86unit,strict MSRV
ARM/x86clippy+fmt PASS. 3frozenpairs против 928:
read1341.115→1320.893,get1678.813→1690.141,14alloc/пересечение/≤2% — timinggain
незаявлен; set+1.49%<2%,cold−0.95%с пересечением. 7localFCpairs/API ALL PASS:
readL21321.722/1807.018,get1673.640/1969.775,set89.905/141.945,cold965.629/2149.070;
scaling7.007/7.627≥6,warm/set0alloc,cold5.009. 93hashes/API и 88beforeafter
проверены,две ownerinline-edits сохранены, exact commit проверяется отдельно.
NativeCI37638928114 baseline928 запущен с отдельным untimedCPUprofilefrozenbinaries; без
причины не перезапускать. Queue§5implemented,qualificationpending;
explicit/specializedownership§6 и nativeL2 остаются Must. Публикация готового 0.4.0 и
GitHub release разрешены владельцем; API8/custom markers/FR01…22/RS1…6/nativeL2/final
source/release gates остаются условиями. Один ADR, остальное в CHANGELOG и
docs/PERFORMANCE.md.

## 2026-10-07: rejected strong-only entry ownership experiment

A controlled experiment replaced the private entry's std Arc with triomphe
0.1.16, preserving snapshots and immediate retirement. Rust 1.88 strict clippy
and all 739 checks passed. Three counterbalanced public-API comparisons found
set 91.7 -> 88.6 ns (3.4%), L2 get_or_set 1474.1 -> 1474.4 ns, and unchanged
allocation counts. Set still missed <=0.75x FC. L2 read differed by less than
one percent and its ranges overlapped. The experiment was reverted; no new
dependency or ready path is retained. Full frozen-source data is archived in
`strong-entry-focused/report.json` among the local performance artifacts.


## 2026-10-07 — L2 coordination ownership and CI documentation fixes

An upgraded weak coordination owner is moved into its lock/fence consumer;
identity reuse no longer rotates the idle sweep queue. Creation still performs
bounded reclamation and explicit maintenance remains available. Only sealed
scalar metadata can use these slots, so a cached user value cannot be destroyed
under the coordination shard guard. Identity, FIFO admission and generation
fences retain their contracts. This was chosen over sweeping on every reuse,
which produces no new idle identity and adds queue writes.

Pinned Rust 1.88 formatting, all-target/all-feature strict clippy and 739 default
checks passed. Three counterbalanced frozen comparisons with default .NET PGO
and separate TC=0 settled all 48 warmup records. L2 read 1163.188 -> 1116.352 ns
(FC 1162.608); L2 get_or_set 1442.210 -> 1397.094 ns (FC 1273.457), nine allocations
in both versions. Read and get_or_set ranges do not overlap. Set 90.638 -> 90.804
ns is noise, FC 108.044; cold ranges overlap. L2 get_or_set and set budgets remain
red. Report: docs/benchmarks/2026-10-07-l2-coordination-owned.json. Hot reads are
frozen and no new dependency or unsafe boundary is introduced.

CI 37671995620 exposes two stale documentation references: the builder link to
removed remove_by_tags and the OpenTelemetry doctest's crate::FactoryError,
which resolves to the external doctest crate. The link was fixed and strict
all-feature rustdoc passed locally. The OpenTelemetry example now uses the
current plain factory Result and Cache::new. These are documentation repairs;
runtime contracts are not weakened. Main merge and publication stay prohibited
until mandatory qualification passes.

After the example repair, all-feature Rust 1.88 tests passed: 804 checks, including
nine doctests. Strict all-target/all-feature clippy and all-feature rustdoc with
warnings denied passed on the same working source. Live Redis qualification
remains a separate mandatory CI job.


## 2026-10-07 — one fallible configured constructor

Removed the public CacheBuilder::build panicking adapter. Existing try_build
remains the sole ordinary configured constructor and retains its original typed
errors. This was selected over changing build to Result and retaining a second
alias: callers already use try_build, and RS-6 names that validation boundary.
Cache::new still builds fixed, valid memory-only defaults without a runtime;
its impossible construction rejection is an internal invariant tripwire.

Compiler diagnostics identified exactly the removed CacheBuilder calls, avoiding
Tokio and other provider builders. Migrated 129 test/fixture/example calls without
changing contract assertions. Examples propagate configuration failure or use
Cache::new for the unconfigured defaults. Builder/rustdoc and migration examples
are updated. This does not change retrieval failure policy or not_modified tags,
whose owner decision remains pending.


The constructor migration passed all 804 all-feature checks on Rust 1.88,
all-target/all-feature strict clippy, formatting and rustdoc with warnings denied.
The completed prior CI run 37671995620 was preserved in PERFORMANCE.md and two
raw repository reports. It is not qualified: default-PGO L2/set ratios fail and
several cold warmups are unsettled. The hot/native failures are recorded without
reopening frozen hot-read optimization. Safety, individual features, dependency
advisories and both package consumers passed; Windows and Linux logs confirm the
platform/runtime jobs fail at the repaired OpenTelemetry doctest. No gate was
waived and no main merge or publication was attempted.
