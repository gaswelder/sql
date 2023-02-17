package main

import "fmt"

type star struct {
	//
}

func (s *star) eval(x Row, g []Row) (Value, error) {
	return Value{}, fmt.Errorf("invalid use of star selector")
}

func (s star) String() string {
	return "*"
}
