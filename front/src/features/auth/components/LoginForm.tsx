import { useState } from "react";
import { login } from '../api';

export function LoginForm({ onSuccess }: { onSuccess: () => void }) {
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");

    return (
        <form className="flex flex-col gap-4">

        </form>
    )
}