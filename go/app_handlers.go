package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
)

type appPostUsersRequest struct {
	Username       string  `json:"username"`
	FirstName      string  `json:"firstname"`
	LastName       string  `json:"lastname"`
	DateOfBirth    string  `json:"date_of_birth"`
	InvitationCode *string `json:"invitation_code"`
}

type appPostUsersResponse struct {
	ID             string `json:"id"`
	InvitationCode string `json:"invitation_code"`
}

func appPostUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostUsersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Username == "" || req.FirstName == "" || req.LastName == "" || req.DateOfBirth == "" {
		writeError(w, http.StatusBadRequest, errors.New("required fields(username, firstname, lastname, date_of_birth) are empty"))
		return
	}

	userID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	invitationCode := secureRandomStr(15)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userID, req.Username, req.FirstName, req.LastName, req.DateOfBirth, accessToken, invitationCode,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 初回登録キャンペーンのクーポンを付与
	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
		userID, "CP_NEW2024", 3000,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 招待コードを使った登録
	if req.InvitationCode != nil && *req.InvitationCode != "" {
		// 招待する側の招待数をチェック
		var coupons []Coupon
		err = tx.SelectContext(ctx, &coupons, "SELECT * FROM coupons WHERE code = ? FOR UPDATE", "INV_"+*req.InvitationCode)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if len(coupons) >= 3 {
			writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
			return
		}

		// ユーザーチェック
		var inviter User
		err = tx.GetContext(ctx, &inviter, "SELECT * FROM users WHERE invitation_code = ?", *req.InvitationCode)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// 招待クーポン付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
			userID, "INV_"+*req.InvitationCode, 1500,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		// 招待した人にもRewardを付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, CONCAT(?, '_', FLOOR(UNIX_TIMESTAMP(NOW(3))*1000)), ?)",
			inviter.ID, "RWD_"+*req.InvitationCode, 1000,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "app_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &appPostUsersResponse{
		ID:             userID,
		InvitationCode: invitationCode,
	})
}

type appPostPaymentMethodsRequest struct {
	Token string `json:"token"`
}

func appPostPaymentMethods(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostPaymentMethodsRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, errors.New("token is required but was empty"))
		return
	}

	user := ctx.Value("user").(*User)

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)`,
		user.ID,
		req.Token,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type getAppRidesResponse struct {
	Rides []getAppRidesResponseItem `json:"rides"`
}

type getAppRidesResponseItem struct {
	ID                    string                       `json:"id"`
	PickupCoordinate      Coordinate                   `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                   `json:"destination_coordinate"`
	Chair                 getAppRidesResponseItemChair `json:"chair"`
	Fare                  int                          `json:"fare"`
	Evaluation            int                          `json:"evaluation"`
	RequestedAt           int64                        `json:"requested_at"`
	CompletedAt           int64                        `json:"completed_at"`
}

type getAppRidesResponseItemChair struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Model string `json:"model"`
}

func appGetRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	rides := []Ride{}
	if err := tx.SelectContext(
		ctx,
		&rides,
		`SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC`,
		user.ID,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	items := []getAppRidesResponseItem{}
	for _, ride := range rides {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "COMPLETED" {
			continue
		}

		fare, err := calculateDiscountedFare(ctx, tx, user.ID, &ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		item := getAppRidesResponseItem{
			ID:                    ride.ID,
			PickupCoordinate:      Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude},
			DestinationCoordinate: Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude},
			Fare:                  fare,
			Evaluation:            *ride.Evaluation,
			RequestedAt:           ride.CreatedAt.UnixMilli(),
			CompletedAt:           ride.UpdatedAt.UnixMilli(),
		}

		item.Chair = getAppRidesResponseItemChair{}

		chair := &Chair{}
		if err := tx.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, ride.ChairID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		item.Chair.ID = chair.ID
		item.Chair.Name = chair.Name
		item.Chair.Model = chair.Model

		owner := &Owner{}
		if err := tx.GetContext(ctx, owner, `SELECT * FROM owners WHERE id = ?`, chair.OwnerID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		item.Chair.Owner = owner.Name

		items = append(items, item)
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &getAppRidesResponse{
		Rides: items,
	})
}

type appPostRidesRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesResponse struct {
	RideID string `json:"ride_id"`
	Fare   int    `json:"fare"`
}

type ExecutableGet interface {
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error

	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

func getLatestRideStatus(ctx context.Context, tx ExecutableGet, rideID string) (string, error) {
	status := ""
	if err := tx.GetContext(ctx, &status, `SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1`, rideID); err != nil {
		return "", err
	}
	return status, nil
}

func appPostRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)
	rideID := ulid.Make().String()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	rides := []Ride{}
	if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE user_id = ?`, user.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	continuingRideCount := 0
	for _, ride := range rides {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "COMPLETED" {
			continuingRideCount++
		}
	}

	if continuingRideCount > 0 {
		writeError(w, http.StatusConflict, errors.New("ride already exists"))
		return
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude)
				  VALUES (?, ?, ?, ?, ?, ?)`,
		rideID, user.ID, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)`,
		ulid.Make().String(), rideID, "MATCHING",
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var rideCount int
	if err := tx.GetContext(ctx, &rideCount, `SELECT COUNT(*) FROM rides WHERE user_id = ? `, user.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var coupon Coupon
	if rideCount == 1 {
		// 初回利用で、初回利用クーポンがあれば必ず使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			// 無ければ他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			} else {
				if _, err := tx.ExecContext(
					ctx,
					"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
					rideID, user.ID, coupon.Code,
				); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = 'CP_NEW2024'",
				rideID, user.ID,
			); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	} else {
		// 他のクーポンを付与された順番に使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
				rideID, user.ID, coupon.Code,
			); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	}

	ride := Ride{}
	if err := tx.GetContext(ctx, &ride, "SELECT * FROM rides WHERE id = ?", rideID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, user.ID, &ride, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusAccepted, &appPostRidesResponse{
		RideID: rideID,
		Fare:   fare,
	})
}

type appPostRidesEstimatedFareRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesEstimatedFareResponse struct {
	Fare     int `json:"fare"`
	Discount int `json:"discount"`
}

func appPostRidesEstimatedFare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesEstimatedFareRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	discounted, err := calculateDiscountedFare(ctx, tx, user.ID, nil, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRidesEstimatedFareResponse{
		Fare:     discounted,
		Discount: calculateFare(req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude) - discounted,
	})
}

// マンハッタン距離を求める
func calculateDistance(aLatitude, aLongitude, bLatitude, bLongitude int) int {
	return abs(aLatitude-bLatitude) + abs(aLongitude-bLongitude)
}
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

type appPostRideEvaluationRequest struct {
	Evaluation int `json:"evaluation"`
}

type appPostRideEvaluationResponse struct {
	CompletedAt int64 `json:"completed_at"`
}

func appPostRideEvaluatation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	req := &appPostRideEvaluationRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Evaluation < 1 || req.Evaluation > 5 {
		writeError(w, http.StatusBadRequest, errors.New("evaluation must be between 1 and 5"))
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE id = ?`, rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status, err := getLatestRideStatus(ctx, tx, ride.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if status != "ARRIVED" {
		writeError(w, http.StatusBadRequest, errors.New("not arrived yet"))
		return
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE rides SET evaluation = ? WHERE id = ?`,
		req.Evaluation, rideID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if count, err := result.RowsAffected(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	} else if count == 0 {
		writeError(w, http.StatusNotFound, errors.New("ride not found"))
		return
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)`,
		ulid.Make().String(), rideID, "COMPLETED")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE id = ?`, rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	paymentToken := &PaymentToken{}
	if err := tx.GetContext(ctx, paymentToken, `SELECT * FROM payment_tokens WHERE user_id = ?`, ride.UserID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusBadRequest, errors.New("payment token not registered"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, ride.UserID, ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	paymentGatewayRequest := &paymentGatewayPostPaymentRequest{
		Amount: fare,
	}

	var paymentGatewayURL string
	if err := tx.GetContext(ctx, &paymentGatewayURL, "SELECT value FROM settings WHERE name = 'payment_gateway_url'"); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := requestPaymentGatewayPostPayment(ctx, paymentGatewayURL, paymentToken.Token, paymentGatewayRequest, func() ([]Ride, error) {
		rides := []Ride{}
		if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE user_id = ? ORDER BY created_at ASC`, ride.UserID); err != nil {
			return nil, err
		}
		return rides, nil
	}); err != nil {
		if errors.Is(err, erroredUpstream) {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRideEvaluationResponse{
		CompletedAt: ride.UpdatedAt.UnixMilli(),
	})
}

type appGetNotificationResponse struct {
	Data         *appGetNotificationResponseData `json:"data"`
	RetryAfterMs int                             `json:"retry_after_ms"`
}

type appGetNotificationResponseData struct {
	RideID                string                           `json:"ride_id"`
	PickupCoordinate      Coordinate                       `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                       `json:"destination_coordinate"`
	Fare                  int                              `json:"fare"`
	Status                string                           `json:"status"`
	Chair                 *appGetNotificationResponseChair `json:"chair,omitempty"`
	CreatedAt             int64                            `json:"created_at"`
	UpdateAt              int64                            `json:"updated_at"`
}

type appGetNotificationResponseChair struct {
	ID    string                               `json:"id"`
	Name  string                               `json:"name"`
	Model string                               `json:"model"`
	Stats appGetNotificationResponseChairStats `json:"stats"`
}

type appGetNotificationResponseChairStats struct {
	TotalRidesCount    int     `json:"total_rides_count"`
	TotalEvaluationAvg float64 `json:"total_evaluation_avg"`
}

func appGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	joinedQuery := `
		select r.*, rs.status from rides r
			left join isuride.ride_statuses rs on r.id = rs.ride_id
			where r.user_id = ?
			order by rs.created_at desc
			limit 1`

	rideAndRideStatus := &RideAndRideStatus{}

	if err := db.GetContext(ctx, rideAndRideStatus, joinedQuery, user.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusOK, &appGetNotificationResponse{
				RetryAfterMs: 30,
			})
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare2(ctx, db, user.ID, rideAndRideStatus, rideAndRideStatus.PickupLatitude, rideAndRideStatus.PickupLongitude, rideAndRideStatus.DestinationLatitude, rideAndRideStatus.DestinationLongitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	response := &appGetNotificationResponse{
		Data: &appGetNotificationResponseData{
			RideID: rideAndRideStatus.ID,
			PickupCoordinate: Coordinate{
				Latitude:  rideAndRideStatus.PickupLatitude,
				Longitude: rideAndRideStatus.PickupLongitude,
			},
			DestinationCoordinate: Coordinate{
				Latitude:  rideAndRideStatus.DestinationLatitude,
				Longitude: rideAndRideStatus.DestinationLongitude,
			},
			Fare:      fare,
			Status:    rideAndRideStatus.Status,
			CreatedAt: rideAndRideStatus.CreatedAt.UnixMilli(),
			UpdateAt:  rideAndRideStatus.UpdatedAt.UnixMilli(),
		},
		RetryAfterMs: 30,
	}

	if rideAndRideStatus.ChairID.Valid {
		chair := &Chair{}
		if err := db.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, rideAndRideStatus.ChairID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		stats, err := getChairStats(ctx, db, chair.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		response.Data.Chair = &appGetNotificationResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			Stats: stats,
		}
	}

	// transactionをここで貼る
	if rideAndRideStatus.RideStatusID != "" {
		tx, err := db.Beginx()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, rideAndRideStatus.RideStatusID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if err := tx.Commit(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	writeJSON(w, http.StatusOK, response)
}

func getChairStats(ctx context.Context, tx ExecutableGet, chairID string) (appGetNotificationResponseChairStats, error) {
	stats := appGetNotificationResponseChairStats{}

	// TODO コレ消せるかも
	rides := []Ride{}
	err := tx.SelectContext(
		ctx,
		&rides,
		`SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC`,
		chairID,
	)
	if err != nil {
		return stats, err
	}

	totalRideCount := 0
	totalEvaluation := 0.0

	selectQuery := `
		select count(*) from ride_statuses rs
			left join rides r on r.id = rs.ride_id
			where chair_id = ?
			and rs.status = 'COMPLETED'`

	// total ride count取得
	err = tx.GetContext(ctx, &totalRideCount, selectQuery, chairID)
	if err != nil {
		return stats, err
	}

	var evaluationCount int64
	evalCountQuery := `
		select sum(rides.evaluation) from rides
			where chair_id = ?`
	err = tx.GetContext(ctx, &evaluationCount, evalCountQuery, chairID)
	totalEvaluation = float64(float32(evaluationCount))

	stats.TotalRidesCount = totalRideCount
	if totalRideCount > 0 {
		stats.TotalEvaluationAvg = totalEvaluation / float64(totalRideCount)
	}

	return stats, nil
}

type appGetNearbyChairsResponse struct {
	Chairs      []appGetNearbyChairsResponseChair `json:"chairs"`
	RetrievedAt int64                             `json:"retrieved_at"`
}

type appGetNearbyChairsResponseChair struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Model             string     `json:"model"`
	CurrentCoordinate Coordinate `json:"current_coordinate"`
}

func appGetNearbyChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	distanceStr := r.URL.Query().Get("distance")
	if latStr == "" || lonStr == "" {
		writeError(w, http.StatusBadRequest, errors.New("latitude or longitude is empty"))
		return
	}

	lat, err := strconv.Atoi(latStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("latitude is invalid"))
		return
	}

	lon, err := strconv.Atoi(lonStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("longitude is invalid"))
		return
	}

	distance := 50
	if distanceStr != "" {
		distance, err = strconv.Atoi(distanceStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("distance is invalid"))
			return
		}
	}

	coordinate := Coordinate{Latitude: lat, Longitude: lon}

	chairs := []Chair{}
	nearbyChairs := []appGetNearbyChairsResponseChair{}

	// 利用可能なイスのリスト
	availableChairsQuery := `
		select c.*
			from chairs c
			left join isuride.rides r on c.id = r.chair_id
			left join isuride.ride_statuses rs on r.id = rs.ride_id
			where c.is_active = 1
			and (rs.status is null OR rs.status = 'COMPLETED')`

	db.SelectContext(ctx, &chairs, availableChairsQuery)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairIds := make([]string, 0)
	for _, chair := range chairs {
		chairIds = append(chairIds, chair.ID)
	}
	//
	//var inQuery string
	//var params []interface{}
	//inQuery, params, err = sqlx.In("select * from chair_locations where chair_id in (?) order by created_at",
	//	chairIds,
	//)
	//if err != nil {
	//	writeError(w, http.StatusInternalServerError, err)
	//	return
	//}
	//
	//// ここでselect文を実行。 inをつかったクエリを db.Rebindする。組立時に取ったパラメータをここで ... をつかって渡す。
	//chairLocation := &ChairLocation{}
	//err = db.SelectContext(ctx, &chairLocation, db.Rebind(inQuery), params...)
	//if err != nil {
	//	writeError(w, http.StatusInternalServerError, err)
	//	return
	//}

	for _, chair := range chairs {

		// 最新の位置情報を取得
		// TODO : IN句で改善できるかも？ -> だめそう
		chairLocation := &ChairLocation{}
		err = db.GetContext(
			ctx,
			chairLocation,
			`SELECT * FROM chair_locations WHERE chair_id = ? ORDER BY created_at DESC LIMIT 1`,
			chair.ID,
		)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if calculateDistance(coordinate.Latitude, coordinate.Longitude, chairLocation.Latitude, chairLocation.Longitude) <= distance {
			nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{
				ID:    chair.ID,
				Name:  chair.Name,
				Model: chair.Model,
				CurrentCoordinate: Coordinate{
					Latitude:  chairLocation.Latitude,
					Longitude: chairLocation.Longitude,
				},
			})
		}
	}

	retrievedAt := &time.Time{}
	err = db.GetContext(
		ctx,
		retrievedAt,
		`SELECT CURRENT_TIMESTAMP(6)`,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
		Chairs:      nearbyChairs,
		RetrievedAt: retrievedAt.UnixMilli(),
	})
}

func calculateFare(pickupLatitude, pickupLongitude, destLatitude, destLongitude int) int {
	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	return initialFare + meteredFare
}

func calculateDiscountedFare(ctx context.Context, db ExecutableGet, userID string, ride *Ride, pickupLatitude, pickupLongitude, destLatitude, destLongitude int) (int, error) {
	var coupon Coupon
	discount := 0
	if ride != nil {
		destLatitude = ride.DestinationLatitude
		destLongitude = ride.DestinationLongitude
		pickupLatitude = ride.PickupLatitude
		pickupLongitude = ride.PickupLongitude

		// すでにクーポンが紐づいているならそれの割引額を参照
		if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", ride.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
		} else {
			discount = coupon.Discount
		}
	} else {
		// 初回利用クーポンを最優先で使う
		if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL", userID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}

			// 無いなら他のクーポンを付与された順番に使う
			if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1", userID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
			} else {
				discount = coupon.Discount
			}
		} else {
			discount = coupon.Discount
		}
	}

	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	discountedMeteredFare := max(meteredFare-discount, 0)

	return initialFare + discountedMeteredFare, nil
}

func calculateDiscountedFare2(ctx context.Context, db ExecutableGet, userID string, ride *RideAndRideStatus, pickupLatitude, pickupLongitude, destLatitude, destLongitude int) (int, error) {
	var coupon Coupon
	discount := 0
	if ride != nil {
		destLatitude = ride.DestinationLatitude
		destLongitude = ride.DestinationLongitude
		pickupLatitude = ride.PickupLatitude
		pickupLongitude = ride.PickupLongitude

		// すでにクーポンが紐づいているならそれの割引額を参照
		if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", ride.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
		} else {
			discount = coupon.Discount
		}
	} else {
		// 初回利用クーポンを最優先で使う
		if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL", userID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}

			// 無いなら他のクーポンを付与された順番に使う
			if err := db.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1", userID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
			} else {
				discount = coupon.Discount
			}
		} else {
			discount = coupon.Discount
		}
	}

	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	discountedMeteredFare := max(meteredFare-discount, 0)

	return initialFare + discountedMeteredFare, nil
}
