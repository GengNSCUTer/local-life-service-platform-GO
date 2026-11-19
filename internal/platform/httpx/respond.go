package httpx

import "github.com/gin-gonic/gin"

type response struct {
	Success  bool   `json:"success"`
	ErrorMsg string `json:"errorMsg,omitempty"`
	Data     any    `json:"data,omitempty"`
}

func Respond(c *gin.Context, status int, message string, data any) {
	c.JSON(status, response{
		Success:  message == "",
		ErrorMsg: message,
		Data:     data,
	})
}
