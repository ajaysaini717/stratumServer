package server

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

type User struct {
	Username  string
	Password  string
	MinerAddr string
}

type Config struct {
	Port string

}

// In-memory store of users
var (
	users   = map[string]*User{}
	usersMu sync.Mutex
	jwtKey  = []byte("123456789uhv")
)

// CORS middleware helper
func setCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

// Signup registers a new user
func signupHandler(w http.ResponseWriter, r *http.Request) {
	if setCORS(w, r) {
		return
	}
	var req struct{ Username, Password string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	usersMu.Lock()
	defer usersMu.Unlock()
	if _, exists := users[req.Username]; exists {
		http.Error(w, "user exists", http.StatusBadRequest)
		return
	}
	users[req.Username] = &User{Username: req.Username, Password: req.Password}
	w.WriteHeader(http.StatusCreated)
}

// Login authenticates and returns a JWT
func loginHandler(w http.ResponseWriter, r *http.Request) {
	if setCORS(w, r) {
		return
	}
	var req struct{ Username, Password string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	usersMu.Lock()
	user, ok := users[req.Username]
	usersMu.Unlock()
	if !ok || user.Password != req.Password {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	// Create token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": req.Username,
		"exp":      time.Now().Add(24 * time.Hour).Unix(),
	})
	tokStr, err := token.SignedString(jwtKey)
	if err != nil {
		http.Error(w, "could not sign token", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"token": tokStr})
}

// AddMiner sets the single MinerAddr for the authenticated user
func addMinerHandler(w http.ResponseWriter, r *http.Request) {
	if setCORS(w, r) {
		return
	}
	username := extractUser(r)
	if username == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req struct{ Address string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	usersMu.Lock()
	defer usersMu.Unlock()
	user, exists := users[username]
	if !exists {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	if user.MinerAddr != "" {
		http.Error(w, "miner address already set", http.StatusBadRequest)
		return
	}
	user.MinerAddr = req.Address
	w.WriteHeader(http.StatusNoContent)
}

// GetMiner returns the registered MinerAddr
func getMinerHandler(w http.ResponseWriter, r *http.Request) {
	if setCORS(w, r) {
		return
	}
	username := extractUser(r)
	if username == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	usersMu.Lock()
	addr := users[username].MinerAddr
	usersMu.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"minerAddr": addr})
}

// extractUser retrieves username from JWT token
func extractUser(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	tokStr := strings.TrimPrefix(auth, "Bearer ")
	token, err := jwt.Parse(tokStr, func(t *jwt.Token) (interface{}, error) {
		return jwtKey, nil
	})
	if err != nil || !token.Valid {
		return ""
	}
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if un, ok2 := claims["username"].(string); ok2 {
			return un
		}
	}
	return ""
}

func Run(cfg Config) error{
	mux := http.NewServeMux()
	mux.HandleFunc("/signup", signupHandler)
	mux.HandleFunc("/login", loginHandler)
	mux.HandleFunc("/miner", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getMinerHandler(w, r)
		case http.MethodPost:
			addMinerHandler(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	
	log.Printf("API server listening on %s", cfg.Port)
	if err := http.ListenAndServe(cfg.Port, mux); err != nil {
		return err
	} else {
		return nil
	}
	
}


