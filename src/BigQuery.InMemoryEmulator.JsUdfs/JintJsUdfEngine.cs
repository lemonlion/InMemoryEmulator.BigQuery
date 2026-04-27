using Jint;
using Jint.Native;

namespace BigQuery.InMemoryEmulator.JsUdfs;

/// <summary>
/// JavaScript UDF engine backed by Jint (a .NET JavaScript interpreter).
/// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure
///   "A JavaScript UDF lets you call code written in JavaScript from a SQL query."
/// </summary>
public class JintJsUdfEngine : IJsUdfEngine
{
	/// <inheritdoc />
	public object? Execute(string body, IReadOnlyList<string> parameterNames, IReadOnlyList<object?> arguments)
	{
		// Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#supported-javascript-udf-data-types
		//   BigQuery maps SQL types to JavaScript types for function parameters.
		var engine = new Engine();

		// Set each parameter as a global variable
		for (int i = 0; i < parameterNames.Count && i < arguments.Count; i++)
		{
			var value = arguments[i];
			engine.SetValue(parameterNames[i], ConvertToJsValue(engine, value));
		}

		// Wrap the body in a function and call it so 'return' statements work
		var wrappedScript = $"(function({string.Join(", ", parameterNames)}) {{ {body} }})({string.Join(", ", parameterNames)})";
		var result = engine.Evaluate(wrappedScript);

		return ConvertFromJsValue(result);
	}

	private static JsValue ConvertToJsValue(Engine engine, object? value)
	{
		return value switch
		{
			null => JsValue.Null,
			bool b => b ? JsBoolean.True : JsBoolean.False,
			int i => new JsNumber(i),
			long l => new JsNumber(l),
			float f => new JsNumber(f),
			double d => new JsNumber(d),
			decimal m => new JsNumber((double)m),
			string s => new JsString(s),
			_ => JsValue.FromObject(engine, value)
		};
	}

	private static object? ConvertFromJsValue(JsValue value)
	{
		if (value.IsNull() || value.IsUndefined())
			return null;
		if (value.IsBoolean())
			return value.AsBoolean();
		if (value.IsNumber())
		{
			var num = value.AsNumber();
			// Return as long if it's a whole number, else double
			if (num == Math.Floor(num) && !double.IsInfinity(num))
				return (long)num;
			return num;
		}
		if (value.IsString())
			return value.AsString();
		if (value.IsArray())
		{
			var arr = value.AsArray();
			var list = new List<object?>();
			foreach (var item in arr)
				list.Add(ConvertFromJsValue(item));
			return list;
		}
		// For objects, convert to string representation
		return value.ToString();
	}
}
