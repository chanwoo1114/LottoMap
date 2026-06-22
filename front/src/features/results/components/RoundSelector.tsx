
interface RoundSelectorProps {
  latest: number;
  value: number;
  onChange: (round: number) => void;
}

export function RoundSelector({ latest, value, onChange }: RoundSelectorProps) {

  return (
    <div>
      <button>
        ‹
      </button>

      <div>

      </div>

      <button>
        ›
      </button>
    </div>
  )
}