// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"fmt"

	"github.com/schollz/progressbar/v3"
)

type Bar interface {
	Add(int) error
	Add64(int64) error
	Current() int64
	SetCurrent(int64) error
	Close() error
}

type ProgressBar struct {
	*progressbar.ProgressBar
}

func (pb *ProgressBar) Current() int64 {
	return pb.ProgressBar.State().CurrentNum
}

func (pb *ProgressBar) SetCurrent(value int64) error {
	return pb.Set64(value)
}

func NewBar(total int64, description, unit string) *ProgressBar {
	return &ProgressBar{
		ProgressBar: progressbar.NewOptions64(total,
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionSetItsString(unit),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionSetWidth(20),
			progressbar.OptionSetPredictTime(true),
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionShowElapsedTimeOnFinish(),
			progressbar.OptionSetDescription(description),
			progressbar.OptionOnCompletion(func() {
				fmt.Printf("\n") //nolint:forbidigo
			}),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
		),
	}
}

func NewBytesBar(totalBytes int64, description string) *ProgressBar {
	return &ProgressBar{
		ProgressBar: progressbar.NewOptions64(totalBytes,
			progressbar.OptionShowCount(),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionSetPredictTime(true),
			progressbar.OptionSetWidth(20),
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowTotalBytes(true),
			progressbar.OptionShowElapsedTimeOnFinish(),
			progressbar.OptionSetDescription(description),
			progressbar.OptionOnCompletion(func() {
				fmt.Printf("\n") //nolint:forbidigo
			}),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
		),
	}
}
