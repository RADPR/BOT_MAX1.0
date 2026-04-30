package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	maxbot "github.com/max-messenger/max-bot-api-client-go"
	"github.com/max-messenger/max-bot-api-client-go/schemes"
	_ "modernc.org/sqlite"

	"github.com/joho/godotenv"
)

var db *sql.DB

const adminID = 178799408   // замените на реальный ID администратора
const managerID = 178799408 // замените на реальный ID менеджера

const (
	consentText    = "Нажимая «Согласен», вы даёте согласие на обработку персональных данных в соответствии с Политикой конфиденциальности ( https://www.medicalsmart.ru/politika-konfidentsialnosti/ )."
	consentVersion = "1.0"
	channel        = "max_bot"
)

func main() {
	_ = godotenv.Load()
	token := os.Getenv("BOT_TOKEN")
	if token == "" {
		log.Fatal("BOT_TOKEN не задан. Укажите токен в переменной окружения или в файле .env")
	}

	api, err := maxbot.New(token)
	if err != nil {
		log.Fatalf("Не удалось создать API: %v", err)
	}

	db, err = sql.Open("sqlite", "./users.db")
	if err != nil {
		log.Fatalf("Ошибка открытия БД: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			user_id      INTEGER PRIMARY KEY,
			phone        TEXT,
			first_name   TEXT,
			last_name    TEXT,
			consent      INTEGER DEFAULT 0,
			consented_at TEXT,
			revoked_at   TEXT,
			state        TEXT DEFAULT 'start'
		)`)
	if err != nil {
		log.Fatalf("Ошибка создания таблицы users: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS consent_events (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id       INTEGER NOT NULL,
			event_type    TEXT    NOT NULL,
			event_time    TEXT    NOT NULL,
			consent_text  TEXT,
			doc_version   TEXT,
			channel       TEXT,
			trigger       TEXT,
			metadata      TEXT,
			recorded_at   TEXT    NOT NULL DEFAULT (datetime('now'))
		)`)
	if err != nil {
		log.Fatalf("Ошибка создания таблицы consent_events: %v", err)
	}

	go dailyBackup()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	log.Println("Бот запущен. Жду сообщений...")

	for update := range api.GetUpdates(ctx) {
		switch upd := update.(type) {

		case *schemes.BotStartedUpdate:
			showWelcome(api, upd.ChatId)

		case *schemes.MessageCreatedUpdate:
			msg := upd.Message

			if strings.TrimSpace(msg.Body.Text) == "" {
				continue
			}
			if msg.Sender.UserId == 0 || msg.Recipient.ChatId == 0 {
				continue
			}

			chatID := msg.Recipient.ChatId
			userID := msg.Sender.UserId
			text := strings.TrimSpace(msg.Body.Text)

			// Административные команды
			if userID == adminID {
				switch {
				case strings.EqualFold(text, "/stats"):
					handleStats(api, chatID)
					continue
				case strings.EqualFold(text, "/export"):
					handleExport(api, chatID)
					continue
				}
			}

			// Пользовательские команды
			switch {
			case strings.EqualFold(text, "/agree") || strings.EqualFold(text, "согласен"):
				metadata := makeMetadata(chatID, userID)
				giveConsent(api, chatID, userID, &msg.Sender, "text_command", metadata)
			case strings.EqualFold(text, "/deny") || strings.EqualFold(text, "не согласен"):
				denyConsent(api, chatID)
			case strings.EqualFold(text, "/revoke") || strings.EqualFold(text, "отозвать согласие"):
				metadata := makeMetadata(chatID, userID)
				revokeConsent(api, chatID, userID, "text_command", metadata)
			default:
				state := getUserState(userID)
				switch state {
				case "awaiting_phone":
					savePhone(api, chatID, userID, text)
				case "consented":
					sendText(api, chatID, "Вы уже дали согласие на обработку персональных данных. Вы можете отозвать его, написав «Отозвать согласие» или /revoke")
				default:
					showInstructions(api, chatID)
				}
			}
		}
	}
}

// ---------- Вспомогательные функции ----------

func makeMetadata(chatID int64, userID int64) string {
	m := map[string]interface{}{
		"chat_id":   chatID,
		"user_id":   userID,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func getUserState(userID int64) string {
	var state string
	err := db.QueryRow("SELECT state FROM users WHERE user_id=?", userID).Scan(&state)
	if err != nil {
		return "start"
	}
	return state
}

// ---------- Приветствие и инструкции ----------

func showWelcome(api *maxbot.Api, chatID int64) {
	text := "Добро пожаловать в клинику СМАРТМЕДИКАЛ 👋\n\n" +
		"Чтобы продолжить запись, нужно ваше согласие на обработку персональных данных.\n\n" +
		consentText + "\n\n" +
		"Выберите вариант:\n" +
		"• Согласен или /agree — продолжить запись\n" +
		"• Не согласен или /deny — отказаться\n" +
		"• Отозвать согласие или /revoke — отозвать ранее данное согласие"
	sendText(api, chatID, text)
}

func showInstructions(api *maxbot.Api, chatID int64) {
	showWelcome(api, chatID)
}

// ---------- Логика согласия ----------

func giveConsent(api *maxbot.Api, chatID int64, userID int64, user *schemes.User, trigger string, metadata string) {
	state := getUserState(userID)
	if state == "awaiting_phone" || state == "consented" {
		sendText(api, chatID, "Вы уже дали согласие на обработку персональных данных. Вы можете отозвать его, написав «Отозвать согласие» или /revoke. Ссылка на менеджера: Вы также можете написать ему напрямую: https://max.ru/u/f9LHodD0cOLrPoe4mY8NfieqXWzB5hhFOd_LcdoRmfPM4mQz9NP1zu_IKOg")
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)

	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE user_id=?)", userID).Scan(&exists)
	if err != nil {
		log.Printf("Ошибка проверки пользователя: %v", err)
		sendText(api, chatID, "Произошла ошибка при сохранении данных.")
		return
	}

	if exists {
		_, err = db.Exec(`UPDATE users SET consent=1, consented_at=?, revoked_at=NULL,
			first_name=?, last_name=?, state='awaiting_phone' WHERE user_id=?`,
			now, user.FirstName, user.LastName, userID)
	} else {
		_, err = db.Exec(`INSERT INTO users
			(user_id, phone, first_name, last_name, consent, consented_at, state)
			VALUES (?, '', ?, ?, 1, ?, 'awaiting_phone')`,
			userID, user.FirstName, user.LastName, now)
	}
	if err != nil {
		log.Printf("Ошибка сохранения согласия: %v", err)
		sendText(api, chatID, "Произошла ошибка при сохранении данных.")
		return
	}

	_, err = db.Exec(`INSERT INTO consent_events 
		(user_id, event_type, event_time, consent_text, doc_version, channel, trigger, metadata) 
		VALUES (?, 'consent_given', ?, ?, ?, ?, ?, ?)`,
		userID, now, consentText, consentVersion, channel, trigger, metadata)
	if err != nil {
		log.Printf("Ошибка записи события согласия: %v", err)
	}

	sendText(api, chatID, "Спасибо! Ваше согласие зафиксировано.\nТеперь, пожалуйста, укажите ваш контактный номер телефона в формате +7XXXXXXXXXX или 8XXXXXXXXXX.")
}

// ---------- Запрос телефона ----------

func savePhone(api *maxbot.Api, chatID int64, userID int64, text string) {
	re := regexp.MustCompile(`^(\+7|8)?[\s\-]?\(?[489][0-9]{2}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{2}$`)
	if !re.MatchString(strings.ReplaceAll(text, " ", "")) {
		sendText(api, chatID, "Номер телефона не распознан. Пожалуйста, отправьте его в формате +7XXXXXXXXXX или 8XXXXXXXXXX.")
		return
	}

	clean := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, text)
	if len(clean) == 11 && clean[0] == '8' {
		clean = "+7" + clean[1:]
	} else if len(clean) == 10 {
		clean = "+7" + clean
	} else {
		clean = "+" + clean
	}

	_, err := db.Exec("UPDATE users SET phone=?, state='consented' WHERE user_id=?", clean, userID)
	if err != nil {
		log.Printf("Ошибка сохранения телефона: %v", err)
		sendText(api, chatID, "Не удалось сохранить номер. Попробуйте ещё раз.")
		return
	}

	var firstName, lastName string
	db.QueryRow("SELECT first_name, last_name FROM users WHERE user_id=?", userID).Scan(&firstName, &lastName)
	notifyText := fmt.Sprintf("✅ Новый пациент дал согласие:\nID: %d\nИмя: %s %s\nТелефон: %s", userID, firstName, lastName, clean)
	sendText(api, managerID, notifyText)

	sendText(api, chatID, "Спасибо! Ваш номер сохранён. Сейчас с вами свяжется менеджер.\nВы также можете написать ему напрямую: https://max.ru/u/f9LHodD0cOLrPoe4mY8NfieqXWzB5hhFOd_LcdoRmfPM4mQz9NP1zu_IKOg")
}

func denyConsent(api *maxbot.Api, chatID int64) {
	sendText(api, chatID,
		"К сожалению, без согласия на обработку персональных данных мы не можем продолжить запись в клинику СМАРТМЕДИКАЛ.")
}

// ---------- Отзыв согласия ----------

func revokeConsent(api *maxbot.Api, chatID int64, userID int64, trigger string, metadata string) {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := db.Exec(`UPDATE users SET consent=0, revoked_at=?, state='start' WHERE user_id=? AND consent=1`,
		now, userID)
	if err != nil {
		log.Printf("Ошибка отзыва согласия: %v", err)
		sendText(api, chatID, "Не удалось отозвать согласие. Попробуйте позже.")
		return
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		sendText(api, chatID, "Вы ещё не давали согласия, или оно уже отозвано.")
		return
	}

	_, err = db.Exec(`INSERT INTO consent_events 
		(user_id, event_type, event_time, consent_text, doc_version, channel, trigger, metadata) 
		VALUES (?, 'consent_revoked', ?, NULL, NULL, NULL, ?, ?)`,
		userID, now, trigger, metadata)
	if err != nil {
		log.Printf("Ошибка записи события отзыва: %v", err)
	}

	sendText(api, chatID, "Согласие отозвано. Если передумаете, вы можете дать его снова, написав \"Согласен\" или /agree")
}

// ---------- Статистика и экспорт ----------

func handleStats(api *maxbot.Api, chatID int64) {
	var total, agreed, denied, revoked int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&total)
	db.QueryRow("SELECT COUNT(*) FROM users WHERE consent=1").Scan(&agreed)
	db.QueryRow("SELECT COUNT(*) FROM users WHERE consent=0 AND consented_at IS NULL").Scan(&denied)
	db.QueryRow("SELECT COUNT(*) FROM users WHERE consent=0 AND revoked_at IS NOT NULL").Scan(&revoked)

	msg := fmt.Sprintf(
		"📊 Статистика:\n"+
			"Всего пользователей: %d\n"+
			"Согласных: %d\n"+
			"Отказавшихся: %d\n"+
			"Отозвавших согласие: %d",
		total, agreed, denied, revoked,
	)
	sendText(api, chatID, msg)
}

func handleExport(api *maxbot.Api, chatID int64) {
	rows, err := db.Query("SELECT user_id, phone, first_name, last_name, consent, consented_at, revoked_at FROM users")
	if err != nil {
		log.Printf("Ошибка запроса к БД: %v", err)
		sendText(api, chatID, "Не удалось получить данные.")
		return
	}
	defer rows.Close()

	dir := "exports"
	os.Mkdir(dir, 0755)
	filename := filepath.Join(dir, fmt.Sprintf("users_%s.csv", time.Now().Format("20060102_150405")))
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Ошибка создания файла: %v", err)
		sendText(api, chatID, "Не удалось создать файл экспорта.")
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"user_id", "phone", "first_name", "last_name", "consent", "consented_at", "revoked_at"})

	for rows.Next() {
		var userID int64
		var phone, firstName, lastName, consentedAt, revokedAt sql.NullString
		var consent int
		if err := rows.Scan(&userID, &phone, &firstName, &lastName, &consent, &consentedAt, &revokedAt); err != nil {
			continue
		}
		writer.Write([]string{
			fmt.Sprintf("%d", userID),
			phoneStr(phone),
			firstName.String,
			lastName.String,
			fmt.Sprintf("%d", consent),
			consentedAt.String,
			revokedAt.String,
		})
	}
	writer.Flush()
	file.Close()

	content, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("Ошибка чтения файла экспорта: %v", err)
		sendText(api, chatID, "Не удалось прочитать данные для экспорта.")
		return
	}

	msg := maxbot.NewMessage().
		SetChat(chatID).
		SetText("Экспорт данных пользователей:\n\n" + string(content))
	sendMessageWithRetry(api, msg)
}

func phoneStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// ---------- Отправка сообщений с ретраем ----------

func sendText(api *maxbot.Api, chatID int64, text string) {
	msg := maxbot.NewMessage().
		SetChat(chatID).
		SetText(text)
	sendMessageWithRetry(api, msg)
}

func sendMessageWithRetry(api *maxbot.Api, msg *maxbot.Message) {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if err := api.Messages.Send(context.Background(), msg); err != nil {
			lastErr = err
			log.Printf("Ошибка отправки (попытка %d): %v", attempt, err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		return
	}
	log.Printf("Не удалось отправить сообщение после 3 попыток: %v", lastErr)
}

// ---------- Ежедневное резервное копирование ----------

func dailyBackup() {
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day(), 3, 0, 0, 0, now.Location())
		if now.After(next) {
			next = next.Add(24 * time.Hour)
		}
		time.Sleep(time.Until(next))

		backupFile := filepath.Join("backups", fmt.Sprintf("users_%s.db", now.Format("20060102")))
		os.Mkdir("backups", 0755)
		src, err := os.Open("users.db")
		if err != nil {
			log.Printf("Ошибка открытия БД для бэкапа: %v", err)
			continue
		}
		dst, err := os.Create(backupFile)
		if err != nil {
			src.Close()
			log.Printf("Ошибка создания бэкапа: %v", err)
			continue
		}
		_, err = io.Copy(dst, src)
		src.Close()
		dst.Close()
		if err != nil {
			log.Printf("Ошибка копирования БД: %v", err)
		} else {
			log.Printf("Бэкап сохранён: %s", backupFile)
		}
	}
}
