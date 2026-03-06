package main
import (
  "context"
  "fmt"
  "os"
  _ "github.com/marcboeker/go-duckdb"
  "github.com/robince/parqview/internal/engine"
  "github.com/robince/parqview/internal/missing"
)
func main(){
  for unique:=1; unique<=400; unique++ {
    for reps:=1; reps<=200; reps++ {
      f, _ := os.CreateTemp("", "distinct-*.csv")
      fmt.Fprintln(f, "category")
      for i:=0;i<unique;i++ {
        for r:=0;r<reps;r++ {
          fmt.Fprintf(f, "value-%03d\n", i)
        }
      }
      fmt.Fprintln(f, "NaN")
      f.Close()
      eng, err := engine.New(f.Name())
      if err != nil { panic(err) }
      s1, _ := eng.ProfileBasic(context.Background(), "category", missing.ModeNullOnly)
      s2, _ := eng.ProfileBasic(context.Background(), "category", missing.ModeNaNOnly)
      eng.Close()
      os.Remove(f.Name())
      if s1.IsDiscrete != s2.IsDiscrete {
        fmt.Printf("unique=%d reps=%d nullOnly distinct=%d pct=%.5f discrete=%v | nanOnly distinct=%d pct=%.5f discrete=%v\n", unique, reps, s1.DistinctApprox, s1.DistinctPct, s1.IsDiscrete, s2.DistinctApprox, s2.DistinctPct, s2.IsDiscrete)
        return
      }
    }
  }
  fmt.Println("none")
}
