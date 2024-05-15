package main

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync"
)

const bearer = "48ab34464a5573519725deb5865cc74c"
const apiSaveFact = "https://development.kpi-drive.ru/_api/facts/save_fact"

type Fact struct {
	PeriodStart         string `json:"period_start"`
	PeriodEnd           string `json:"period_end"`
	PeriodKey           string `json:"period_key"`
	IndicatorToMoID     int    `json:"indicator_to_mo_id"`
	IndicatorToMoFactID int    `json:"indicator_to_mo_fact_id"`
	Value               int    `json:"value"`
	FactTime            string `json:"fact_time"`
	IsPlan              int    `json:"is_plan"`
	AuthUserID          int    `json:"auth_user_id"`
	Comment             string `json:"comment"`
}

// saveFact сохраняет один факт на сервере
func saveFact(fact Fact, token string) error {
	req, err := http.NewRequest(http.MethodPost, apiSaveFact, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for key, value := range map[string]string{
		"period_start":            fact.PeriodStart,
		"period_end":              fact.PeriodEnd,
		"period_key":              fact.PeriodKey,
		"indicator_to_mo_id":      fmt.Sprintf("%v", fact.IndicatorToMoID),
		"indicator_to_mo_fact_id": fmt.Sprintf("%v", fact.IndicatorToMoFactID),
		"value":                   fmt.Sprintf("%v", fact.Value),
		"fact_time":               fact.FactTime,
		"is_plan":                 fmt.Sprintf("%v", fact.IsPlan),
		"auth_user_id":            fmt.Sprintf("%v", fact.AuthUserID),
		"comment":                 fact.Comment,
	} {
		_ = writer.WriteField(key, value)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("error closing form data writer: %v", err)
	}

	req.Body = io.NopCloser(body)

	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status code: %d", resp.StatusCode)
	}

	return nil
}

// generateFacts создает слайс тестовых фактов
func generateFacts(numFacts int) []Fact {
	facts := make([]Fact, 0, numFacts)
	for i := 1; i <= numFacts; i++ {
		fact := Fact{
			PeriodStart:         "2024-05-01",
			PeriodEnd:           "2024-05-31",
			PeriodKey:           "month",
			IndicatorToMoID:     227373,
			IndicatorToMoFactID: 0,
			Value:               i,
			FactTime:            "2024-05-31",
			IsPlan:              0,
			AuthUserID:          40,
			Comment:             fmt.Sprintf("Buffer Loginov: %v", i),
		}
		facts = append(facts, fact)
	}
	return facts
}

// processFacts отправляет факты в буфер и запускает горутину обработки
func processFacts(facts []Fact, buffer chan Fact, wg *sync.WaitGroup) {
	for _, fact := range facts {
		buffer <- fact
	}
	close(buffer)
	wg.Wait()
}

// processFactsBuffer создаёт буфер через который отправляет факты
func processFactsBuffer(facts []Fact, batchSize int) {
	buffer := make(chan Fact, len(facts))

	// Запускаем горутину для сохранения фактов
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for fact := range buffer {
			err := saveFact(fact, bearer)
			if err != nil {
				fmt.Println("Error saving fact:", err)
			} else {
				fmt.Println("Fact saved:", fact)
			}
		}
	}()

	for i := 0; i < batchSize; i++ {
		processFacts(facts, buffer, &wg)
	}
}

func main() {
	facts := generateFacts(1000)
	processFactsBuffer(facts, 10)

	fmt.Println("All facts processed")
}
